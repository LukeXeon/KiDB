package utils

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPriorityQueueMinMax 小顶/大顶堆弹出序（归并器的核心不变式）。
func TestPriorityQueueMinMax(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	perm := rand.Perm(1000)

	minq := NewMinPriorityQueue[int, int]()
	maxq := NewMaxPriorityQueue[int, int]()
	for _, v := range perm {
		minq.Push(v, v)
		maxq.Push(v, v)
	}
	require.Equal(t, 1000, minq.Len())

	prev := -1
	for minq.Len() > 0 {
		v, p := minq.Pop()
		require.Equal(t, v, p)
		require.Greater(t, v, prev)
		prev = v
	}
	require.Equal(t, 999, prev)

	prev = 10000
	for maxq.Len() > 0 {
		v, _ := maxq.Pop()
		require.Less(t, v, prev)
		prev = v
	}
	require.Equal(t, 0, prev)
	_ = rng
}

// TestPriorityQueueString 字符串优先级（字典序归并形态）。
func TestPriorityQueueString(t *testing.T) {
	q := NewMinPriorityQueue[int, string]()
	words := []string{"shenzhen", "shanghai", "shaoxing", "beijing", "shangrao"}
	for i, w := range words {
		q.Push(i, w)
	}
	var got []string
	for q.Len() > 0 {
		_, p := q.Pop()
		got = append(got, p)
	}
	require.True(t, sort.StringsAreSorted(got))
	require.Equal(t, "beijing", got[0])
}

// TestPriorityQueuePeek 堆顶窥探。
func TestPriorityQueuePeek(t *testing.T) {
	q := NewMinPriorityQueue[string, float64]()
	_, _, ok := q.Peek()
	require.False(t, ok)
	q.Push("a", 2.5)
	q.Push("b", 1.5)
	_, p, ok := q.Peek()
	require.True(t, ok)
	require.Equal(t, 1.5, p)
	require.Equal(t, 2, q.Len()) // 不弹出
}

// TestStrings 归一 ZRANGE 族两形态。
func TestStrings(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, Strings([]string{"a", "b"}))
	require.Equal(t, []string{"1", "x"}, Strings([]any{1, "x"}))
	require.Nil(t, Strings(nil))
}

// TestStringMap 归一 HGETALL 三形态 + 截断报错。
func TestStringMap(t *testing.T) {
	m, err := StringMap(map[string]string{"k": "v"})
	require.NoError(t, err)
	require.Equal(t, "v", m["k"])

	m, err = StringMap(map[any]any{"k": 1})
	require.NoError(t, err)
	require.Equal(t, "1", m["k"])

	m, err = StringMap([]any{"a", "1", "b", "2"})
	require.NoError(t, err)
	require.Len(t, m, 2)

	_, err = StringMap([]any{"a"})
	require.Error(t, err) // 奇数长度 = 截断

	m, err = StringMap(nil)
	require.NoError(t, err)
	require.Empty(t, m)

	_, err = StringMap(42)
	require.Error(t, err)
}

// BenchmarkPriorityQueue 16384 路种子规模（top-k 归并的真实形状）。
func BenchmarkPriorityQueue(b *testing.B) {
	for i := 0; i < b.N; i++ {
		q := NewMinPriorityQueue[int, float64]()
		for j := 0; j < 16384; j++ {
			q.Push(j, float64(j%997))
		}
		for q.Len() > 0 {
			q.Pop()
		}
	}
}

func ExamplePriorityQueue() {
	q := NewMinPriorityQueue[string, int]()
	q.Push("b", 2)
	q.Push("a", 1)
	v, p := q.Pop()
	fmt.Println(v, p)
	// Output: a 1
}
