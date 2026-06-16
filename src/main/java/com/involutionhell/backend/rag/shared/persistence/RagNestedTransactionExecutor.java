package com.involutionhell.backend.rag.shared.persistence;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class RagNestedTransactionExecutor {

    private final TransactionTemplate transactionTemplate;

    public RagNestedTransactionExecutor(PlatformTransactionManager transactionManager) {
        if (transactionManager instanceof AbstractPlatformTransactionManager platformTransactionManager) {
            platformTransactionManager.setNestedTransactionAllowed(true);
        }
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
    }

    public void executeWithIntegrityRetry(Runnable operation) {
        try {
            execute(operation);
        } catch (DataIntegrityViolationException exception) {
            execute(operation);
        }
    }

    private void execute(Runnable operation) {
        transactionTemplate.executeWithoutResult(status -> operation.run());
    }
}
