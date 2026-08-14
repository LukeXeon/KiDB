// kidb-server 是 KiDB 的 MySQL 协议网关进程（docs/11 §11.2：唯一产品形态）。
//
// 当前为骨架：引导参数解析与内核接线点已就位；
// 协议层（go-mysql-server wire server）与参考适配器（adapter/goredis）
// 随实现期依赖接入（docs/02 §2.1、docs/10 §10.1）。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"kidb"
)

func main() {
	var (
		listen       = flag.String("listen", ":3306", "MySQL 协议监听地址")
		addrs        = flag.String("redis", "127.0.0.1:6379", "Redis Cluster 地址，逗号分隔")
		poolSize     = flag.Int("pool", 128, "连接池大小")
		readTimeout  = flag.Duration("read-timeout", 3*time.Second, "读超时（scatter 预算 = 读超时 × headroom）")
		writeTimeout = flag.Duration("write-timeout", 3*time.Second, "写超时")
		rwOnly       = flag.Bool("read-write-only", false, "纯读写节点：不启动后台角色循环（docs/08 §8.5 豁免）")
	)
	flag.Parse()

	boot := kidb.Bootstrap{
		Addrs:         strings.Split(*addrs, ","),
		PoolSize:      *poolSize,
		ReadTimeout:   *readTimeout,
		WriteTimeout:  *writeTimeout,
		ReadWriteOnly: *rwOnly,
		ListenAddr:    *listen,
	}

	slog.Info("kidb-server bootstrap",
		"listen", boot.ListenAddr, "redis", boot.Addrs, "rwOnly", boot.ReadWriteOnly)

	// TODO(impl)：构建期链接适配器（默认 adapter/goredis）→ kidb.NewKernel(boot)
	// → 挂载 Catalog 驱动的 go-mysql-server DatabaseProvider → 启动 wire server。
	fmt.Fprintln(os.Stderr, "kidb-server: 内核接线待实现期依赖接入（docs/02）。")
}
