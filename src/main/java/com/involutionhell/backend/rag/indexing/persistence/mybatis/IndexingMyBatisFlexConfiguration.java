package com.involutionhell.backend.rag.indexing.persistence.mybatis;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@MapperScan(basePackageClasses = RagChunkMapper.class)
class IndexingMyBatisFlexConfiguration {
}
