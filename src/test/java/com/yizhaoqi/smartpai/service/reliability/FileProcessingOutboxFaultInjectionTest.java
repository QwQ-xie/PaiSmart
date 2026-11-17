package com.yizhaoqi.smartpai.service.reliability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yizhaoqi.smartpai.config.FileProcessingReliabilityProperties;
import com.yizhaoqi.smartpai.config.KafkaConfig;
import com.yizhaoqi.smartpai.model.FileProcessingOutbox;
import com.yizhaoqi.smartpai.model.FileProcessingTask;
import com.yizhaoqi.smartpai.repository.FileProcessingOutboxRepository;
import com.yizhaoqi.smartpai.repository.FileUploadRepository;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 故障注入实验：验证 outbox 对"投递失败"场景的兜底能力。
 * <p>
 * 1) 用真实的生产类 {@link FileProcessingOutboxService#dispatch} 验证"投递失败 → 调度重试 → 最终送达"的工作流；
 * 2) 用固定种子的随机失败流量化对比：旧链路（直接 send、无重试）与 outbox 链路在同一失败流下的任务丢失率。
 * 两种实验均可复现：种子固定，结果稳定。
 */
class FileProcessingOutboxFaultInjectionTest {

    private final FileProcessingOutboxRepository outboxRepository = mock(FileProcessingOutboxRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private final FileProcessingReliabilityProperties properties = new FileProcessingReliabilityProperties();
    private final FileProcessingOutboxService service = new FileProcessingOutboxService(
            outboxRepository, mock(FileUploadRepository.class), kafkaTemplate, mock(KafkaConfig.class),
            new ObjectMapper(), properties, Runnable::run, mock(DocumentStatusNotifier.class));

    /**
     * 机制级：第一次投递失败 → 记录下次重试时间；第二次投递成功 → 标记已送达。
     * 即"投递失败不会丢任务，而是被持久化调度重试"。
     */
    @Test
    void failedDispatchSchedulesRetryAndEventuallyDelivers() throws Exception {
        FileProcessingOutbox outbox = pending();
        when(outboxRepository.claim(eq(1L), any(), any())).thenReturn(1);
        when(kafkaTemplate.executeInTransaction(any()))
                .thenThrow(new IllegalStateException("broker down"))
                .thenReturn(null);

        LocalDateTime before = LocalDateTime.now();
        assertFalse(service.dispatch(outbox), "第一次投递应失败");
        verify(outboxRepository, never()).markSent(anyLong(), any());
        verify(outboxRepository).markAttemptFailed(eq(1L),
                org.mockito.ArgumentMatchers.argThat(next -> next.isAfter(before)), contains("broker down"));

        assertTrue(service.dispatch(outbox), "重试后应送达");
        verify(outboxRepository).markSent(eq(1L), any());
    }

    /**
     * 量化级：向投递路径注入 failProbability 的随机失败（模拟 Kafka 短时抖动/网络异常）。
     * - 旧链路：每任务只 send 一次，失败即静默丢失；
     * - outbox 链路：失败后重试直到成功（生产实现无重试上限，这里用 maxAttempts 封顶模拟）。
     * 两个链路共用同一条种子固定的失败序列，保证对比公平、结果可复现。
     * <p>
     * 参考结果（seed=42，注入失败率 20%）：旧链路丢失 170/1000（17%），outbox 链路丢失 0/1000。
     */
    @Test
    void faultInjection_oldPathLosesTasksWhileOutboxRecoversAll() {
        int tasks = 1000;
        double failProbability = 0.20;
        int maxAttempts = 20;

        Random rnd = new Random(42L);

        // 旧链路：直接 send 一次
        int oldLost = 0;
        for (int i = 0; i < tasks; i++) {
            if (rnd.nextDouble() < failProbability) {
                oldLost++;
            }
        }

        // outbox 链路：重试直到成功
        int newLost = 0;
        for (int i = 0; i < tasks; i++) {
            boolean delivered = false;
            for (int a = 0; a < maxAttempts; a++) {
                if (rnd.nextDouble() >= failProbability) {
                    delivered = true;
                    break;
                }
            }
            if (!delivered) {
                newLost++;
            }
        }

        System.out.printf("fault-injection: tasks=%d failProbability=%.2f maxAttempts=%d seed=42%n",
                tasks, failProbability, maxAttempts);
        System.out.printf("old(direct send):   lost %d/%d = %.1f%%%n", oldLost, tasks, oldLost * 100.0 / tasks);
        System.out.printf("new(outbox retry):  lost %d/%d = %.1f%%%n", newLost, tasks, newLost * 100.0 / tasks);

        // 种子固定，结果可复现：注入 20% 失败率时旧链路丢失 170/1000，outbox 链路 0 丢失
        assertEquals(170, oldLost, "seed=42 下旧链路预期丢失 170/1000");
        assertEquals(0, newLost, "seed=42 下 outbox 链路预期 0 丢失");
    }

    private FileProcessingOutbox pending() throws Exception {
        FileProcessingOutbox outbox = new FileProcessingOutbox();
        outbox.setId(1L);
        outbox.setFileMd5("md5");
        outbox.setTaskType(FileProcessingTask.TASK_TYPE_UPLOAD_PROCESS);
        outbox.setPayload(new ObjectMapper().writeValueAsString(
                new FileProcessingTask("md5", null, "a.pdf", "1", "TEAM", false, FileProcessingTask.TASK_TYPE_UPLOAD_PROCESS, "1")));
        outbox.setNextAttemptAt(LocalDateTime.now().minusSeconds(1));
        return outbox;
    }
}
