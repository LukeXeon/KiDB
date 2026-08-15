package gateway

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/pingcap/tidb/pkg/parser"

	"kidb"
	"kidb/config"
	"kidb/controller"
	"kidb/ddl"
	"kidb/engine"
)

// Server 是 KiDB 网关进程内组装体（docs/02 SQL 服务器）：
// gms wire server + 前置分类拦截（DDL/事务/ro 执法）+ 会话注册表。
type Server struct {
	srv  *server.Server
	deps engine.Deps
	cfg  *config.Store // 配置管理面（docs/10 §10.2）

	plans *planCache // 判定缓存（指纹 + schema 版本绑定，docs/02 §2.6）

	mu       sync.Mutex
	sessions map[uint32]*sessRec // connID → 会话状态

	// 后台角色（docs/08 §8.5；ReadWriteOnly 豁免）
	roleCancel context.CancelFunc
	elector    *controller.Elector
	manager    *controller.Manager
	jobrunner  *controller.JobRunner
}

type sessRec struct {
	sess  sql.Session
	role  string // "rw" / "ro"（docs/02 §2.9）
	ns    string // USE 记录的命名空间（v1 扁平，记录不加前缀）
	conn2 *mysql.Conn
}

// NewServer 组装网关：引擎 + wire server + 分类拦截 wrapper。
func NewServer(deps engine.Deps, boot kidb.Bootstrap) (*Server, error) {
	return newServerWithListener(deps, boot, nil)
}

// newServerWithListener 允许注入自定义 listener（测试走随机端口）。
func newServerWithListener(deps engine.Deps, boot kidb.Bootstrap, l net.Listener) (*Server, error) {
	eng, _, err := engine.Build(deps)
	if err != nil {
		return nil, err
	}

	// 账号注册（gms MySQLDb 提供 MySQL 兼容认证；ro/rw 执法在拦截层）
	if len(boot.Accounts) > 0 {
		mdb := eng.Analyzer.Catalog.MySQLDb
		ed := mdb.Editor()
		for _, acc := range boot.Accounts {
			mdb.AddSuperUser(ed, acc.User, acc.Host, acc.Password)
		}
		ed.Close()
	}

	s := &Server{
		deps:     deps,
		sessions: map[uint32]*sessRec{},
		cfg:      config.New(deps.Client, deps.Reg, "kidb-server"),
		plans:    newPlanCache(1024),
	}

	// 会话构造：BaseSession + 角色登记
	roleOf := map[string]string{}
	for _, acc := range boot.Accounts {
		roleOf[acc.User] = acc.Role
	}
	sb := func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		sess := sql.NewBaseSessionWithClientServer(addr, sql.Client{
			Address: conn.RemoteAddr().String(),
			User:    conn.User,
		}, conn.ConnectionID)
		role := roleOf[conn.User]
		if role == "" {
			role = "rw" // 未声明账号（空账号表场景）默认读写
		}
		s.mu.Lock()
		s.sessions[conn.ConnectionID] = &sessRec{sess: sess, role: role, conn2: conn}
		s.mu.Unlock()
		return sess, nil
	}

	cfg := server.Config{
		Protocol: "tcp",
		Address:  boot.ListenAddr,
		Version:  "8.0.30-KiDB", // 版本伪装（docs/02 §2.10）
		Listener: l,
	}
	if boot.TLSCertFile != "" && boot.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(boot.TLSCertFile, boot.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("gateway: TLS 证书加载: %w", err)
		}
		cfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	wrapper := func(h mysql.Handler) (mysql.Handler, error) {
		return &kidbHandler{Handler: h, s: s}, nil
	}

	srv, err := server.NewServerWithHandler(cfg, eng, sql.NewContext, sb, nil, wrapper)
	if err != nil {
		return nil, fmt.Errorf("gateway: new server: %w", err)
	}
	s.srv = srv
	// 后台角色自动选举（docs/08 §8.5：默认参与，ReadWriteOnly 显式豁免）
	if !boot.ReadWriteOnly {
		s.startRoles(context.Background())
	}
	return s, nil
}

// Start 启动监听（阻塞）。
func (s *Server) Start() error { return s.srv.Start() }

// Close 停服：先停后台角色再关协议层。
func (s *Server) Close() error {
	if s.roleCancel != nil {
		s.roleCancel()
	}
	return s.srv.Close()
}

// kidbHandler 包装 gms Handler：前置分类（docs/02 §2.2）+ 事务拒绝 + ro 执法。
type kidbHandler struct {
	mysql.Handler // 其余方法全部委托
	s             *Server
}

// ComInitDB 记录命名空间并委托（docs/02 §2.5：USE 接受并记录）。
func (h *kidbHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	if rec := h.s.session(c.ConnectionID); rec != nil {
		h.s.mu.Lock()
		rec.ns = schemaName
		h.s.mu.Unlock()
	}
	return h.Handler.ComInitDB(c, schemaName)
}

// ConnectionClosed 清理会话注册。
func (h *kidbHandler) ConnectionClosed(c *mysql.Conn) {
	h.s.mu.Lock()
	delete(h.s.sessions, c.ConnectionID)
	h.s.mu.Unlock()
	h.Handler.ConnectionClosed(c)
}

// ComQuery 命令分发：事务拒绝 → ro 执法 → DDL 路径 → 其余委托引擎。
// 全程计时 → 慢查询日志（docs/10 §10.4：超阈值记录 + 全扫强制告警）。
func (h *kidbHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	start := time.Now()
	route, fullscan, rows := "engine", false, 0
	var qerr error
	defer func() { h.slowQuery(ctx, query, route, fullscan, rows, time.Since(start), qerr) }()
	cb := func(res *sqltypes.Result, more bool) error {
		if res != nil {
			rows += len(res.Rows)
		}
		return callback(res, more)
	}

	// 事务语句：缓存定位不支持（docs/01 §1.2、docs/02 §2.1）
	if isTxnStmt(query) {
		route = "rejected"
		qerr = mysql.NewSQLError(1235, "HY000", "KiDB 不支持事务语句（缓存定位，docs/02 §2.1）")
		return qerr
	}
	// ro 账号执法（docs/02 §2.9）
	if rec := h.s.session(c.ConnectionID); rec != nil && rec.role == "ro" && isWriteStmt(query) {
		route = "rejected"
		qerr = mysql.NewSQLError(1290, "HY000", "只读账号禁止写操作（ERR_READ_ONLY）")
		return qerr
	}

	// 配置管理面（SET GLOBAL / SHOW VARIABLES LIKE，docs/10 §10.2）
	if handled, err := h.handleConfigStmt(ctx, c, query, cb); handled {
		route = "config"
		qerr = err
		return err
	}

	if Classify(query) != RouteDDL {
		// EXPLAIN 接管（docs/02 §2.8：KiDB 计划展示；必须在快速路径之前——
		// EXPLAIN SELECT COUNT(*) 不得真的执行）
		if handled, err := h.explainQuery(ctx, query, cb); handled {
			route = "explain"
			qerr = err
			return err
		}

		// 快筛：SELECT/UPDATE/DELETE 才需要快速路径/守卫判定（其余直通引擎）
		needsGuard := false
		if w := leadingWords(stripComments(query), 1); len(w) > 0 {
			switch w[0] {
			case "SELECT", "UPDATE", "DELETE":
				needsGuard = true
			}
		}
		if !needsGuard {
			qerr = h.Handler.ComQuery(ctx, c, query, cb)
			return qerr
		}

		// plan cache（docs/02 §2.6）：指纹 + schema 版本绑定的判定缓存——
		// 命中即跳过双解析器判定（TiDB parser 的 fastpath+guard 联合评估）。
		_, digestObj := parser.NormalizeDigest(query)
		digest := digestObj.String()
		schemaVer, verr := h.s.deps.Cache.SchemaVersion(ctx)
		if verr == nil {
			if pd, hit, stale := h.s.plans.get(digest, schemaVer); hit {
				if m := h.s.deps.Exec.Metrics(); m != nil {
					m.PlanCacheHit.Inc()
				}
				if pd.fp != nil {
					if res, ok2, err := h.tryFastPath(ctx, pd.fp); err != nil {
						qerr = err
						return sqlErr(err)
					} else if ok2 {
						route = "fastpath:" + pd.fp.table
						qerr = cb(res, false)
						return qerr
					}
				}
				qerr = h.Handler.ComQuery(ctx, c, query, cb)
				return qerr
			} else if stale {
				if m := h.s.deps.Exec.Metrics(); m != nil {
					m.PlanCacheStale.Inc()
				}
			}
		}

		// miss：单 parse 联合评估（快速路径形状 + 有界性执法）
		fp, fs, gerr, parsed := h.analyzeDML(ctx, query)
		if parsed {
			if fp != nil {
				if res, hit, err := h.tryFastPath(ctx, fp); err != nil {
					qerr = err
					return sqlErr(err)
				} else if hit {
					route = "fastpath:" + fp.table
					// fp 命中即守卫豁免（COUNT(*)/MIN/MAX 的扇出本身有界）——
					// 重放只消费 fp，不消费守卫判定，故 fp 命中总是可缓存
					if verr == nil {
						h.s.plans.put(digest, planDecision{schemaVer: schemaVer, fp: fp})
					}
					qerr = cb(res, false)
					return qerr
				}
			}
			if gerr != nil {
				qerr = gerr
				return sqlErr(gerr)
			}
			fullscan = fs
			// 仅缓存"配置无关的放行"（索引谓词/JOIN 档位）；全扫依赖判定
			// （hint/白名单）随 query_allow_fullscan_tables 漂移，不进缓存
			if verr == nil && !fs {
				h.s.plans.put(digest, planDecision{schemaVer: schemaVer, fp: fp})
			}
		}
		qerr = h.Handler.ComQuery(ctx, c, query, cb)
		return qerr
	}

	// DDL 路径：TiDB parser 解析 → 校验 → Catalog 作业
	route = "ddl"
	op, err := ddl.Parse(query)
	if err != nil {
		qerr = err
		return sqlErr(err)
	}
	sqlCtx := h.sqlCtx(ctx, c, query)
	if err := ExecDDL(sqlCtx, op, h.s.deps); err != nil {
		qerr = err
		return sqlErr(err)
	}
	qerr = cb(&sqltypes.Result{}, false)
	return qerr
}

// ComPrepare 预处理：与 ComQuery 同套的执法面（此前为缺口——预处理语句
// 绕过事务拒绝/ro/守卫直达引擎）。分类/事务/ro/守卫在 PREPARE 期判定；
// EXECUTE 由 gms 自己的预处理注册表承载（fastpath 不进预处理路径——
// 引擎执行结果同样正确，加速只对文本协议）。
func (h *kidbHandler) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	if isTxnStmt(query) {
		return nil, mysql.NewSQLError(1235, "HY000", "KiDB 不支持事务语句（缓存定位，docs/02 §2.1）")
	}
	if rec := h.s.session(c.ConnectionID); rec != nil && rec.role == "ro" && isWriteStmt(query) {
		return nil, mysql.NewSQLError(1290, "HY000", "只读账号禁止写操作（ERR_READ_ONLY）")
	}
	if Classify(query) == RouteDDL {
		return nil, mysql.NewSQLError(1235, "HY000", "DDL 不支持预处理协议（低频管理面，走文本协议）")
	}
	// 守卫判定（模板含 ? 占位符：TiDB parser 解析为 ParamMarkerExpr——
	// 列名收集不受影响；LIKE 参数模式保守按无索引处理，见 docs/04 §4.5 注记）
	if _, _, gerr, parsed := h.analyzeDML(ctx, query); parsed && gerr != nil {
		return nil, sqlErr(gerr)
	}
	return h.Handler.ComPrepare(ctx, c, query, prepare)
}

// sqlCtx 为 DDL 路径构造 gms 上下文。
func (h *kidbHandler) sqlCtx(ctx context.Context, c *mysql.Conn, query string) *sql.Context {
	if rec := h.s.session(c.ConnectionID); rec != nil {
		return sql.NewContext(ctx, sql.WithSession(rec.sess), sql.WithQuery(query))
	}
	return sql.NewContext(ctx, sql.WithQuery(query))
}

// session 取会话注册。
func (s *Server) session(connID uint32) *sessRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[connID]
}

// sqlErr 把内核错误翻译为 MySQL 错误码（docs/02 §2.9 错误码映射表）。
func sqlErr(err error) error {
	return mysql.NewSQLError(kidb.MySQLCode(err), "HY000", "%s", err.Error())
}

// isTxnStmt 判定事务语句（恒拒绝）。
func isTxnStmt(query string) bool {
	words := leadingWords(stripComments(query), 2)
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "XA":
		return true
	case "START":
		return len(words) > 1 && words[1] == "TRANSACTION"
	case "LOCK", "UNLOCK": // LOCK TABLES 同属事务语义
		return true
	}
	return false
}

// isWriteStmt 判定写语句（ro 执法用）：写/DDL/SET GLOBAL。
func isWriteStmt(query string) bool {
	words := leadingWords(stripComments(query), 2)
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE":
		return true
	case "SET":
		return len(words) > 1 && words[1] == "GLOBAL"
	}
	return false
}

// leadingWords/stripComments 见 classify.go。
