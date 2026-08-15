// Package ds 是 KiDB 的通用数据结构与回复归一工具包（v6.0 工具收敛面，
// docs/01 §1.6）：泛型优先队列（container/heap 封装，替代第三方微库）、
// Redis 回复形态归一等跨包共享的小工具。只放真正通用的部分。
package ds

import (
	"cmp"
	"container/heap"
)

// PriorityQueue 泛型优先队列（container/heap 的类型安全封装）。
// min=true 小顶堆（最小优先级先出），false 大顶堆。
//
// 使用契约：Pop 空前必须 Len()>0（空 Pop = 编程错误，panic）——
// 归并循环以 Len 为守卫，与 container/heap 裸用相比消掉全部类型断言。
type PriorityQueue[T any, P cmp.Ordered] struct {
	items pqItems[T, P]
	min   bool
}

// cmpOrdered 优先级约束（数值/字符串）。
type cmpOrdered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

// NewMinPriorityQueue 小顶堆（最小优先级先出）。
func NewMinPriorityQueue[T any, P cmp.Ordered]() *PriorityQueue[T, P] {
	return &PriorityQueue[T, P]{min: true}
}

// NewMaxPriorityQueue 大顶堆（最大优先级先出）。
func NewMaxPriorityQueue[T any, P cmp.Ordered]() *PriorityQueue[T, P] {
	return &PriorityQueue[T, P]{min: false}
}

// Len 堆里条目数。
func (q *PriorityQueue[T, P]) Len() int { return len(q.items) }

// Push 入堆。
func (q *PriorityQueue[T, P]) Push(v T, p P) {
	heap.Push(&q.items, pqItem[T, P]{v: v, p: p, max: !q.min})
}

// Pop 弹出最优条目（空队列 = 编程错误，panic）。
func (q *PriorityQueue[T, P]) Pop() (T, P) {
	it := heap.Pop(&q.items).(pqItem[T, P])
	return it.v, it.p
}

// Peek 看堆顶（不弹出）。
func (q *PriorityQueue[T, P]) Peek() (T, P, bool) {
	if len(q.items) == 0 {
		var zeroT T
		var zeroP P
		return zeroT, zeroP, false
	}
	it := q.items[0]
	return it.v, it.p, true
}

// pqItems 实现 container/heap（max 标记决定方向）。
type pqItem[T any, P cmp.Ordered] struct {
	v   T
	p   P
	max bool
}

type pqItems[T any, P cmp.Ordered] []pqItem[T, P]

func (h pqItems[T, P]) Len() int { return len(h) }

func (h pqItems[T, P]) Less(i, j int) bool {
	if h[i].max {
		return h[i].p > h[j].p
	}
	return h[i].p < h[j].p
}

func (h pqItems[T, P]) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *pqItems[T, P]) Push(x any) { *h = append(*h, x.(pqItem[T, P])) }

func (h *pqItems[T, P]) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = pqItem[T, P]{} // 防内存泄漏
	*h = old[:n-1]
	return it
}
