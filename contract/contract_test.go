// Package contract 是适配器一致性测试套件（docs/12 §12.4）：
// 真实 Redis（cluster-enabled，单节点全 slot 覆盖）上校验契约 R1~R7 与错误分类。
// 单节点集群即可强制执行 CROSSSLOT/路由语义；MOVED 场景需多节点，见 CI 编排。
//
// 本地无 docker 时整体跳过；CI 必须带 docker 跑全量（docs/12 §12.9 门禁）。
package contract

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"kidb/keycodec"
	"kidb/kv"
	"kidb/kv/goredis"
	"kidb/script"
)

func dockerAvailable() bool {
	return exec.Command("docker", "info").Run() == nil
}

// startClusterNode 起单节点集群并覆盖全部 16384 slot。
func startClusterNode(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		Cmd:          []string{"redis-server", "--cluster-enabled", "yes", "--appendonly", "no"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("起容器: %v", err)
	}
	// 单节点接管全部 slot（Redis 7 ADDSLOTSRANGE）
	code, out, err := c.Exec(ctx, []string{"redis-cli", "cluster", "addslotsrange", "0", "16383"})
	if err != nil || code != 0 {
		t.Fatalf("addslotsrange: %v %v (code %d)", err, out, code)
	}
	host, _ := c.Host(ctx)
	mapped, _ := c.MappedPort(ctx, "6379")
	return fmt.Sprintf("%s:%s", host, mapped.Port()), func() { _ = c.Terminate(ctx) }
}

func setup(t *testing.T) (kv.Client, context.Context, func()) {
	t.Helper()
	if !dockerAvailable() {
		t.Skip("docker 不可用——契约套件在 CI（带 docker）运行，docs/12 §12.9")
	}
	addr, cleanup := startClusterNode(t)
	cli := goredis.New([]string{addr}, goredis.Options{PoolSize: 16})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	return cli, ctx, func() { cancel(); cli.Close(); cleanup() }
}

// R1：slot 计算一致性——CLUSTER KEYSLOT == keycodec.Slot。
func TestR1SlotConsistency(t *testing.T) {
	cli, ctx, done := setup(t)
	defer done()
	for _, k := range []string{"d:u:{1}", "i:t:i=v:{s:00042}#b0", "foo{bar}zap", "no-tag", "a{}{b}", "x{y}"} {
		res, err := cli.Do(ctx, "CLUSTER", "KEYSLOT", k)
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT: %v", err)
		}
		var got int64
		fmt.Sscan(fmt.Sprint(res), &got)
		if int(got) != int(keycodec.Slot(k)) {
			t.Fatalf("R1 违例: KEYSLOT(%q)=%d, keycodec=%d", k, got, keycodec.Slot(k))
		}
	}
}

// R2：内核/适配器拒绝无 key 命令。
func TestR2KeylessRejected(t *testing.T) {
	cli, ctx, done := setup(t)
	defer done()
	if _, err := cli.Do(ctx, "PING"); err == nil {
		t.Fatal("R2 违例：无 key 命令必须被拒绝")
	}
	if _, err := cli.Pipeline(ctx, []kv.Cmd{{Name: "PING"}}); err == nil {
		t.Fatal("R2 违例：pipeline 内无 key 命令必须被拒绝")
	}
}

// R3：同 slot 多 key Lua 成功；跨 slot Lua 返回 CROSSSLOT。
func TestR3EvalSlotRules(t *testing.T) {
	cli, ctx, done := setup(t)
	defer done()
	sc, err := script.Load()
	if err != nil {
		t.Fatal(err)
	}
	lr, _ := sc.Get("lock_release")

	// 同 slot：行 key 与其 slot 的 exp/桶 key
	slot := keycodec.Slot("d:t:{1}")
	k1 := "d:t:{1}"
	k2 := keycodec.ExpKey("t", slot)
	if keycodec.Slot(k1) != keycodec.Slot(k2) {
		t.Fatalf("测试构造错误：%s 与 %s 不同 slot", k1, k2)
	}
	if _, err := cli.Eval(ctx, lr, []string{k1}, "tok"); err != nil {
		t.Fatalf("同 slot EVAL: %v", err)
	}

	// 跨 slot：应返回 CROSSSLOT 错误（真实集群语义校验）
	m, _ := sc.Get("lock_release")
	_ = m
	other := "d:t:{2}"
	if keycodec.Slot(other) == slot {
		other = "d:t:{3}"
	}
	_, err = cli.Eval(ctx, lr, []string{k1, other}, "tok")
	if err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("跨 slot EVAL 必须 CROSSSLOT，got %v", err)
	}
}

// R4：跨 slot 混合 pipeline 按序返回、无串扰。
func TestR4PipelineOrder(t *testing.T) {
	cli, ctx, done := setup(t)
	defer done()
	var cmds []kv.Cmd
	for i := 0; i < 200; i++ {
		cmds = append(cmds, kv.Cmd{Name: "SET", Args: []any{fmt.Sprintf("ct:{%d}", i), fmt.Sprint(i)}})
	}
	results, err := cli.Pipeline(ctx, cmds)
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	for i, r := range results {
		if fmt.Sprint(r) != "OK" {
			t.Fatalf("元素 %d 异常: %v", i, r)
		}
	}
	// 读回抽查
	gets := []kv.Cmd{{Name: "GET", Args: []any{"ct:{0}"}}, {Name: "GET", Args: []any{"ct:{199}"}}}
	results, _ = cli.Pipeline(ctx, gets)
	if fmt.Sprint(results[0]) != "0" || fmt.Sprint(results[1]) != "199" {
		t.Fatalf("读回串扰: %v", results)
	}
}

// R6：ctx 取消即时返回。
func TestR6ContextCancel(t *testing.T) {
	cli, _, done := setup(t)
	defer done()
	c2, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cli.Do(c2, "GET", "x:{1}"); err == nil {
		t.Fatal("取消的 ctx 必须返回错误（契约 R6）")
	}
}

// R7：SCRIPT FLUSH 后 EVALSHA 自动回退 EVAL。
func TestR7NoscriptFallback(t *testing.T) {
	cli, ctx, done := setup(t)
	defer done()
	sc, err := script.Load()
	if err != nil {
		t.Fatal(err)
	}
	lr, _ := sc.Get("lock_release")
	if _, err := cli.Eval(ctx, lr, []string{"lk:ct"}, "tok"); err != nil {
		t.Fatalf("首次 Eval: %v", err)
	}
	if _, err := cli.Do(ctx, "SCRIPT", "FLUSH"); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if _, err := cli.Eval(ctx, lr, []string{"lk:ct"}, "tok"); err != nil {
		t.Fatalf("NOSCRIPT 回退失败: %v", err)
	}
}
