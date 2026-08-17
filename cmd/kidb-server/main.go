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

	"github.com/Xuanwo/go-locale"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"kidb"
	"kidb/di"
	"kidb/i18n"
)

func main() {
	var (
		listen       = flag.String("listen", ":3306", i18n.T("cli.flag_listen"))
		addrs        = flag.String("redis", "127.0.0.1:6379", i18n.T("cli.flag_redis"))
		poolSize     = flag.Int("pool", 128, i18n.T("cli.flag_pool"))
		readTimeout  = flag.Duration("read-timeout", 3*time.Second, i18n.T("cli.flag_read_timeout"))
		writeTimeout = flag.Duration("write-timeout", 3*time.Second, i18n.T("cli.flag_write_timeout"))
		replicaRead  = flag.Bool("replica-read", false, i18n.T("cli.flag_replica_read"))
		metricsAddr  = flag.String("metrics-addr", "", i18n.T("cli.flag_metrics_addr"))
		accounts     = flag.String("accounts", "root:127.0.0.1:kidb:rw", i18n.T("cli.flag_accounts"))
		lang         = flag.String("lang", "", i18n.T("cli.flag_lang"))
	)
	flag.Parse()

	// 语言：--lang 显式指定优先；未指定按系统语言环境自动探测（go-locale）
	langVal := *lang
	if langVal == "" {
		if tag, err := locale.Detect(); err == nil {
			if base, _ := tag.Base(); base.String() == "zh" {
				langVal = i18n.LangChinese
			}
		}
	}

	boot := kidb.Bootstrap{
		Lang:         langVal,
		Addrs:        strings.Split(*addrs, ","),
		PoolSize:     *poolSize,
		ReadTimeout:  *readTimeout,
		WriteTimeout: *writeTimeout,
		ReplicaRead:  *replicaRead,
		ListenAddr:   *listen,
	}
	if !isFlagSet("accounts") {
		slog.Warn(i18n.T("cli.default_account_warn")) // 出厂默认账号仅本机可达；生产必须显式配置
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

	i18n.SetLang(boot.Lang)

	// DI 装配（唯一入口，docs/01 §1.6；指标暴露为进程级可选端点）
	srv, err := di.InitializeServer(boot)
	if err != nil {
		slog.Error(i18n.T("cli.start_failed"), "err", err)
		os.Exit(1)
	}
	if *metricsAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				slog.Error(i18n.T("cli.metrics_exit"), "err", err)
			}
		}()
	}

	slog.Info(i18n.T("cli.started"), "listen", boot.ListenAddr, "redis", boot.Addrs)
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// isFlagSet 报告 flag 是否被显式设置。
func isFlagSet(name string) bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}
