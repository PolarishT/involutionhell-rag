package com.involutionhell.backend.rag.document.persistence.mybatis;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@MapperScan(basePackageClasses = RagDocumentMapper.class)
class DocumentMyBatisFlexConfiguration {
}
