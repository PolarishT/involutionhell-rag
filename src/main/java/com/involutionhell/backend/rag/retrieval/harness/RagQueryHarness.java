package com.involutionhell.backend.rag.retrieval.harness;

import reactor.core.publisher.Flux;

/**
 * Query 链路的轻量 Harness。
 *
 * <p>它负责把 query transformation、query expansion、retrieval、join 和上下文邻居扩展
 * 组织成一个可观测的阶段流，应用服务只需要把阶段事件翻译成对外 SSE 契约。
 */
public interface RagQueryHarness {

    Flux<RagQueryHarnessEvent> execute(RagQueryHarnessRequest request);
}
