package com.involutionhell.backend.rag.retrieval.persistence.mybatis;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@MapperScan(basePackageClasses = RagAskRunMapper.class)
class RetrievalMyBatisFlexConfiguration {
}
