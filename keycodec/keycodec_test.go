package keycodec

import (
	"strings"
	"testing"
)

// 行内聚（v7.0 收窄）：只有回执与异步日志必须与行同 slot（行 Lua 原子面）。
func TestRowColocation(t *testing.T) {
	table, pk := "users", "u_9527"
	rowSlot := Slot(RowKey(table, pk))
	if Slot(ReceiptKey(table, pk)) != rowSlot {
		t.Fatal("receipt must colocate with row")
	}
	if Slot(AsyncLogKey(table, "idx_a", rowSlot)) != rowSlot {
		t.Fatal("async log must colocate with row slot")
	}
	// v7.0：登记册集中，不与行同 slot（形态钉死）
	if k := ExpKey(table); k != "exp:users" {
		t.Fatalf("ExpKey = %q", k)
	}
	if k := ExpShardKey(table, 3); k != "exp:users#3" {
		t.Fatalf("ExpShardKey = %q", k)
	}
	if k := ExpKeyN(table, 1, 4); k != "exp:users#1" {
		t.Fatalf("ExpKeyN = %q", k)
	}
	if k := ExpKeyN(table, 0, 1); k != "exp:users" {
		t.Fatalf("ExpKeyN 单册 = %q", k)
	}
}

// 等值桶 v7 形态：默认 tag={encVal}；子桶 #b{n} 编入 tag（摊异 slot）。
func TestEqBucketForm(t *testing.T) {
	k0 := EqBucketKey("users", "idx_city", "shanghai", 0)
	if k0 != "i:users:idx_city={shanghai}" {
		t.Fatalf("默认桶 = %q", k0)
	}
	k3 := EqBucketKey("users", "idx_city", "shanghai", 3)
	if k3 != "i:users:idx_city={shanghai#b3}" {
		t.Fatalf("子桶 = %q", k3)
	}
	if Slot(k3) == Slot(k0) {
		t.Fatal("子桶必须摊异 slot（crc16 不同 tag）")
	}
	// 子桶选择确定性 + 值域
	for _, pk := range []string{"1", "2", "u_9527"} {
		n := EqSubFor(pk, 8)
		if n < 0 || n >= 8 {
			t.Fatalf("EqSubFor(%q,8)=%d 越界", pk, n)
		}
		if EqSubFor(pk, 8) != n {
			t.Fatal("EqSubFor 必须确定")
		}
	}
	if EqSubFor("1", 1) != 0 || EqSubFor("1", 0) != 0 {
		t.Fatal("K≤1 恒 0")
	}
}

// 范围/lex 桶 v7 形态。
func TestRangeLexBucketForm(t *testing.T) {
	if k := RangeBucketKey("users", "idx_age", 0); k != "i:users:idx_age:{r0}" {
		t.Fatalf("range 默认桶 = %q", k)
	}
	if k := RangeBucketKey("users", "idx_age", 5); k != "i:users:idx_age:{r5}" {
		t.Fatalf("range 子桶 = %q", k)
	}
	if k := LexBucketKey("users", "idx_city", 2); k != "i:users:idx_city:{l2}" {
		t.Fatalf("lex 子桶 = %q", k)
	}
	if Slot(RangeBucketKey("users", "idx_age", 0)) == Slot(RangeBucketKey("users", "idx_age", 1)) {
		t.Fatal("范围子桶必须摊异 slot")
	}
}

// 唯一预约 key：同值必同 key 同 slot（docs/05 §5.3）。
func TestUniqueKeyDeterministic(t *testing.T) {
	k1 := UniqueKey("users", "idx_email", "alice@x.com")
	k2 := UniqueKey("users", "idx_email", "alice@x.com")
	if k1 != k2 || Slot(k1) != Slot(k2) {
		t.Fatal("same value must map to same reservation key/slot")
	}
	if k3 := UniqueKey("users", "idx_email", "bob@x.com"); k3 == k1 {
		t.Fatal("different values must not collide (pre-image)")
	}
}

// L4 副本必须落在与源桶不同的 slot 且确定性可寻址（docs/08 §8.4）。
func TestReplicaOffSlot(t *testing.T) {
	src := EqBucketKey("users", "idx_city", "shanghai", 0)
	const stride = 16384 / 9
	seen := map[uint16]bool{Slot(src): true}
	for k := 1; k <= 8; k++ {
		rep := ReplicaKey(src, k)
		want := (Slot(src) + uint16(k*stride)) % NumSlots
		if Slot(rep) != want {
			t.Fatalf("replica %d slot = %d, want %d", k, Slot(rep), want)
		}
		if seen[Slot(rep)] {
			t.Fatalf("replica %d collides with another slot", k)
		}
		seen[Slot(rep)] = true
	}
}

// TestEscapeValueDigest 摘要桶规则（docs/03 §3.2）：
// 超长/含结构字符的值走 xxhash64 摘要（~x 前缀），同值必同 key。
func TestEscapeValueDigest(t *testing.T) {
	// 常规值：URL escape 直通
	if got := EscapeValue("shanghai"); got != "shanghai" {
		t.Fatalf("EscapeValue = %q", got)
	}
	// 含冒号：摘要
	a := EscapeValue("has:colon")
	if !HasDigestPrefix(a) {
		t.Fatalf("含冒号值应摘要，got %q", a)
	}
	// 超长：摘要
	long := make([]byte, 200)
	for i := range long {
		long[i] = 'x'
	}
	if !HasDigestPrefix(EscapeValue(string(long))) {
		t.Fatal("超长值应摘要")
	}
	// 同值同摘要（唯一约束同值必同 key 的前提）
	if EscapeValue("has:colon") != a {
		t.Fatal("同值必须同摘要")
	}
	// 转义形态绝不含 tag 结构字符（等值桶 tag = {encVal} 的前提，v7.0）
	for _, v := range []string{"has:colon", "{brace}", "hash#tag", string(long)} {
		if strings.ContainsAny(EscapeValue(v), ":{}#") {
			t.Fatalf("EscapeValue(%q) 含结构字符", v)
		}
	}
}

// 桶 key 生成→反解往返（契约钉死：manager 复核经 ParseEqBucketKey 还原候选——
// 尾缀解析边界曾静默拒解析导致分裂复核全灭）。
func TestBucketKeyParseRoundTrip(t *testing.T) {
	for _, sub := range []int{0, 3, 17} {
		eq := EqBucketKey("users", "idx_city", "hot", sub)
		table, idx, encVal, n, ok := ParseEqBucketKey(eq)
		if !ok || table != "users" || idx != "idx_city" || encVal != "hot" || n != sub {
			t.Fatalf("eq 往返错位: %q → %v %v %v %v %v", eq, table, idx, encVal, n, ok)
		}
		rg := RangeBucketKey("users", "idx_age", sub)
		table, idx, n, ok = ParseRangeBucketKey(rg)
		if !ok || table != "users" || idx != "idx_age" || n != sub {
			t.Fatalf("range 往返错位: %q → %v %v %v %v", rg, table, idx, n, ok)
		}
	}
	// 畸形形态显式拒绝（与既有 malformed 分支同纪律）
	if _, _, _, _, ok := ParseEqBucketKey("i:users:idx_city={hot#b-1}"); ok {
		t.Fatal("负数子桶必须 ok=false")
	}
	if _, _, _, _, ok := ParseEqBucketKey("i:users:idx_city={hot#x3}"); ok {
		t.Fatal("非法子桶尾缀必须 ok=false")
	}
	if _, _, _, ok := ParseRangeBucketKey("i:users:idx_age:{r-1}"); ok {
		t.Fatal("负数子桶必须 ok=false")
	}
	if _, _, _, ok := ParseRangeBucketKey("i:users:idx_age:{x1}"); ok {
		t.Fatal("非法前缀必须 ok=false")
	}
	// 含结构字符的值经转义后仍可往返
	v := "has:colon{and}brace"
	eq := EqBucketKey("users", "idx_city", v, 2)
	_, _, encVal, n, ok := ParseEqBucketKey(eq)
	if !ok || encVal != EscapeValue(v) || n != 2 {
		t.Fatalf("转义值往返错位: %q → %q n=%d ok=%v", eq, encVal, n, ok)
	}
}
