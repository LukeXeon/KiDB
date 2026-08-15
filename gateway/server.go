// Package gateway 是 KiDB 的 MySQL 协议网关——**纯装配层**（v6.0 纪律，
// docs/02 §2.2 单引擎纪律）：引擎构造 + wire server + 账号/角色/变量注册 +
// 后台角色启动。**本包不做任何 SQL 文本处理**（无解析/分流/预解析优化——
// 一切语义落在 gms 扩展点：Database/Table/Index/Session/系统变量）。
package gateway

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"

	"kidb"
	"kidb/config"
	"kidb/controller"
	"kidb/engine"
)

// Server 是 KiDB 网关进程内组装体（docs/02）：gms wire server + 后台角色。
type Server struct {
	srv  *server.Server
	deps engine.Deps
	cfg  *config.Store // 配置存储（SET GLOBAL 持久化桥 + 远端变更轮询，docs/10 §10.2）

	// 后台角色（docs/08 §8.5；ReadWriteOnly 豁免）
	roleCancel context.CancelFunc
	elector    *controller.Elector
	manager    *controller.Manager
	jobrunner  *controller.JobRunner
}

// NewServer 组装网关：引擎 + wire server + 账号/变量/后台角色。
func NewServer(deps engine.Deps, boot kidb.Bootstrap) (*Server, error) {
	return newServerWithListener(deps, boot, nil)
}

// newServerWithListener 允许注入自定义 listener（测试走随机端口）。
func newServerWithListener(deps engine.Deps, boot kidb.Bootstrap, l net.Listener) (*Server, error) {
	// 全扫闸门（docs/07 §7.4）：引擎层全扫的唯一执法点
	deps.FullscanGate = engine.NewFullscanGate(deps.Exec.Metrics())
	eng, _, err := engine.Build(deps)
	if err != nil {
		return nil, err
	}

	// 账号注册（gms MySQLDb 提供 MySQL 兼容认证；ro/rw 执法在引擎扩展点）
	if len(boot.Accounts) > 0 {
		mdb := eng.Analyzer.Catalog.MySQLDb
		ed := mdb.Editor()
		for _, acc := range boot.Accounts {
			mdb.AddSuperUser(ed, acc.User, acc.Host, acc.Password)
		}
		ed.Close()
	}

	s := &Server{
		deps: deps,
		cfg:  config.New(deps.Client, deps.Reg, "kidb-server"),
	}

	// 会话构造：engine.Session（角色 + 配置存储句柄；事务显式拒绝内建于类型）
	roleOf := map[string]string{}
	for _, acc := range boot.Accounts {
		roleOf[acc.User] = acc.Role
	}
	sb := func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		role := roleOf[conn.User]
		if role == "" {
			role = "rw" // 未声明账号（空账号表场景）默认读写
		}
		return &engine.Session{
			BaseSession: sql.NewBaseSessionWithClientServer(addr, sql.Client{
				Address: conn.RemoteAddr().String(),
				User:    conn.User,
			}, conn.ConnectionID),
			Role: role,
			Cfg:  s.cfg,
		}, nil
	}

	cfg := server.Config{
		Protocol: "tcp",
		Address:  boot.ListenAddr,
		Version:  "8.0.30-KiDB", // 版本伪装（docs/02 §2.9）
		Listener: l,
	}
	if boot.TLSCertFile != "" && boot.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(boot.TLSCertFile, boot.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("gateway: TLS 证书加载: %w", err)
		}
		cfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	srv, err := server.NewServer(cfg, eng, sql.NewContext, sb, nil)
	if err != nil {
		return nil, fmt.Errorf("gateway: new server: %w", err)
	}
	s.srv = srv

	// 配置即数据（docs/10 §10.2）：启动种子（cfg:global → gms 注册表）+
	// 版本轮询（远端 SET GLOBAL 秒级收敛到本实例）
	if err := s.syncSysvars(context.Background()); err != nil {
		return nil, fmt.Errorf("gateway: 配置种子加载: %w", err)
	}
	go s.pollSysvars(context.Background())

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

// syncSysvars 把 cfg:global 的显式设置回填本实例 gms 注册表
// （SetGlobal 触发的 NotifyChanged 在无 Cfg 会话下跳过回写——值本就来自 cfg:global）。
func (s *Server) syncSysvars(ctx context.Context) error {
	for _, name := range []string{engine.VarFullscanAllowlist, engine.VarReplicaRead, engine.VarRowCache} {
		v, set, err := s.cfg.Get(ctx, name)
		if err != nil {
			return err
		}
		if !set {
			continue
		}
		var val any = v
		if name != engine.VarFullscanAllowlist {
			val = v == "true"
		}
		if err := sql.SystemVariables.SetGlobal(sql.NewContext(ctx), name, val); err != nil {
			return fmt.Errorf("gateway: 回填变量 %s: %w", name, err)
		}
	}
	return nil
}

// pollSysvars 配置版本轮询（与 schema lease 同节奏，秒级传播）。
func (s *Server) pollSysvars(ctx context.Context) {
	var last uint64
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ver, err := s.cfg.Version(ctx)
			if err != nil {
				continue
			}
			if ver != last {
				last = ver
				_ = s.syncSysvars(ctx) // 错误不致命：下轮再来
			}
		}
	}
}
