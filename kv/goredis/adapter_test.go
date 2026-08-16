package goredis_test

import (
	"context"
	"fmt"
	"testing"

	"kidb/kv"
	"kidb/testutil"
)

func TestDoAndPipeline(t *testing.T) {
	cli, _, _ := testutil.New(t)
	ctx := context.Background()

	if _, err := cli.Do(ctx, "HSET", "d:t:{1}", "name", "alice"); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	res, err := cli.Do(ctx, "HGETALL", "d:t:{1}")
	if err != nil {
		t.Fatalf("HGETALL: %v", err)
	}
	m2, ok := res.(map[string]string)
	if !ok || m2["name"] != "alice" {
		t.Fatalf("HGETALL = %v", res)
	}

	// 契约 R2：无 key 命令被适配器拦截
	if _, err := cli.Do(ctx, "PING"); err == nil {
		t.Fatal("Do without key must be rejected (R2)")
	}

	// Pipeline：跨 slot 混合、按序返回（契约 R4）
	results, err := cli.Pipeline(ctx, []kv.Cmd{
		{Name: "HSET", Args: []any{"d:t:{2}", "a", "1"}},
		{Name: "HGET", Args: []any{"d:t:{1}", "name"}},
		{Name: "GET", Args: []any{"no:such:{key}"}}, // redis.Nil → nil
	})
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if len(results) != 3 || results[1] != "alice" || results[2] != nil {
		t.Fatalf("Pipeline results = %v", results)
	}
	got, err := cli.Do(ctx, "HEXISTS", "d:t:{2}", "a")
	if err != nil || fmt.Sprint(got) != "1" {
		t.Fatalf("pipelined HSET not applied: %v %v", got, err)
	}
}

func TestEvalFallbackAndProbe(t *testing.T) {
	cli, reg, _ := testutil.New(t)
	ctx := context.Background()

	lr, _ := reg.Get("lock_release")
	// 首次执行（无脚本缓存）应自动回退 EVAL（契约 R7）
	res, err := cli.Eval(ctx, lr, []string{"lk:test"}, "tok")
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if fmt.Sprint(res) != "0" { // token 不匹配 → 0
		t.Fatalf("lock_release = %v, want 0", res)
	}

	if _, err := cli.Do(ctx, "SET", "lk:test", "tok", "PX", 10000); err != nil {
		t.Fatal(err)
	}
	res, err = cli.Eval(ctx, lr, []string{"lk:test"}, "tok")
	if err != nil || fmt.Sprint(res) != "1" {
		t.Fatalf("lock_release own token = %v, %v; want 1", res, err)
	}
}

func TestDoReplicaUnsupported(t *testing.T) {
	cli, _, _ := testutil.New(t)
	if _, err := cli.DoReplica(context.Background(), "GET", "k"); err == nil {
		t.Fatal("DoReplica without capability must error")
	}
	if cli.Capabilities().ReplicaRead {
		t.Fatal("ReplicaRead should be off by default")
	}
}
