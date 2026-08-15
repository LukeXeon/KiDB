package gateway

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"

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
		// TODO(impl): TLSConfig 由 boot.TLSCertFile/TLSKeyFile 构造
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
func (h *kidbHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	// 事务语句：缓存定位不支持（docs/01 §1.2、docs/02 §2.1）
	if isTxnStmt(query) {
		return mysql.NewSQLError(1235, "HY000", "KiDB 不支持事务语句（缓存定位，docs/02 §2.1）")
	}
	// ro 账号执法（docs/02 §2.9）
	if rec := h.s.session(c.ConnectionID); rec != nil && rec.role == "ro" && isWriteStmt(query) {
		return mysql.NewSQLError(1290, "HY000", "只读账号禁止写操作（ERR_READ_ONLY）")
	}

	// 配置管理面（SET GLOBAL / SHOW VARIABLES LIKE，docs/10 §10.2）
	if handled, err := h.handleConfigStmt(ctx, c, query, callback); handled {
		return err
	}

	if Classify(query) != RouteDDL {
		// KiDB 侧物理快速路径（白名单形状：COUNT(*)/MIN/MAX，docs/04 §4.1/§4.5）
		if fp := matchFastPath(query); fp != nil {
			if res, hit, err := h.tryFastPath(ctx, fp); err != nil {
				return sqlErr(err)
			} else if hit {
				return callback(res, false)
			}
		}
		return h.Handler.ComQuery(ctx, c, query, callback)
	}

	// DDL 路径：TiDB parser 解析 → 校验 → Catalog 作业
	op, err := ddl.Parse(query)
	if err != nil {
		return sqlErr(err)
	}
	sqlCtx := h.sqlCtx(ctx, c, query)
	if err := ExecDDL(sqlCtx, op, h.s.deps); err != nil {
		return sqlErr(err)
	}
	return callback(&sqltypes.Result{}, false)
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
