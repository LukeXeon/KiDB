// kidb-server 是 KiDB 的 MySQL 协议网关进程（docs/11 §11.2：唯一产品形态）。
//
// 接线：引导参数 → DI 图（di.InitializeServer，google/wire 编译期注入）——
// 适配器/内核/引擎/网关/后台角色全部组件由 DI 装配，本文件只解析参数。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"kidb"
	"kidb/di"
)

func main() {
	var (
		listen       = flag.String("listen", ":3306", "MySQL 协议监听地址")
		addrs        = flag.String("redis", "127.0.0.1:6379", "Redis Cluster 地址，逗号分隔")
		poolSize     = flag.Int("pool", 128, "连接池大小")
		readTimeout  = flag.Duration("read-timeout", 3*time.Second, "读超时（scatter 预算 = 读超时 × headroom）")
		writeTimeout = flag.Duration("write-timeout", 3*time.Second, "写超时")
		rwOnly       = flag.Bool("read-write-only", false, "纯读写节点：不启动后台角色循环（docs/08 §8.5 豁免）")
		replicaRead  = flag.Bool("replica-read", false, "声明副本读能力（docs/09 §9.4：适配器构造 ReadOnly 副本客户端；运行时还需 SET GLOBAL replica_read=true 才进读路径）")
		metricsAddr  = flag.String("metrics-addr", "", "Prometheus /metrics HTTP 监听地址（如 :9100；空 = 不暴露）")
		accounts     = flag.String("accounts", "root:%:kidb:rw", "账号表：user:host:pass:role，逗号分隔")
	)
	flag.Parse()

	boot := kidb.Bootstrap{
		Addrs:         strings.Split(*addrs, ","),
		PoolSize:      *poolSize,
		ReadTimeout:   *readTimeout,
		WriteTimeout:  *writeTimeout,
		ReplicaRead:   *replicaRead,
		ReadWriteOnly: *rwOnly,
		ListenAddr:    *listen,
	}
	for _, a := range strings.Split(*accounts, ",") {
		parts := strings.SplitN(a, ":", 4)
		if len(parts) != 4 {
			continue
		}
		boot.Accounts = append(boot.Accounts, kidb.Account{
			User: parts[0], Host: parts[1], Password: parts[2], Role: parts[3],
		})
	}

	// DI 装配（唯一入口，docs/01 §1.6；指标暴露为进程级可选端点）
	srv, err := di.InitializeServer(boot)
	if err != nil {
		slog.Error("kidb-server 启动失败", "err", err)
		os.Exit(1)
	}
	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				slog.Error("metrics 端点退出", "err", err)
			}
		}()
	}

	slog.Info("kidb-server 启动", "listen", boot.ListenAddr, "redis", boot.Addrs, "rwOnly", boot.ReadWriteOnly)
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
