package com.example.asyncqueue.controller;

import com.example.asyncqueue.entity.TaskEntity;
import com.example.asyncqueue.entity.TaskResultEntity;
import com.example.asyncqueue.enums.TaskStatus;
import com.example.asyncqueue.repository.TaskRepository;
import com.example.asyncqueue.repository.TaskResultRepository;
import com.example.asyncqueue.service.AbTestBucketService;
import com.example.asyncqueue.service.TaskSubmitService;
import com.example.asyncqueue.util.BusinessKeyUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * AB-Test 任务接口。
 */
@RestController
@RequestMapping("/task")
public class TaskController {

    private final TaskSubmitService taskSubmitService;
    private final TaskRepository taskRepository;
    private final TaskResultRepository taskResultRepository;
    private final AbTestBucketService abTestBucketService;
    private final ObjectMapper objectMapper;

    public TaskController(TaskSubmitService taskSubmitService,
                          TaskRepository taskRepository,
                          TaskResultRepository taskResultRepository,
                          AbTestBucketService abTestBucketService,
                          ObjectMapper objectMapper) {
        this.taskSubmitService = taskSubmitService;
        this.taskRepository = taskRepository;
        this.taskResultRepository = taskResultRepository;
        this.abTestBucketService = abTestBucketService;
        this.objectMapper = objectMapper;
    }

    /**
     * 提交分桶任务：内部根据 uid + 排序后的实验列表生成 businessKey，保证重复提交幂等。
     */
    @PostMapping("/submit")
    public SubmitResponse submit(@RequestParam String uid,
                                 @RequestParam List<String> expIdList,
                                 @RequestParam(defaultValue = "5") int priority,
                                 @RequestParam(defaultValue = "3") int maxRetry) {
        Assert.hasText(uid, "uid 不能为空");
        Assert.isTrue(priority >= 0 && priority <= 9, "priority 必须在 0~9 之间");
        Assert.isTrue(maxRetry >= 0, "maxRetry 不能小于 0");

        List<String> expIds = normalizeExpIds(expIdList);
        String normalizedUid = uid.trim();

        // businessKey 与 hash key 使用同一套规范：uid 加前缀，实验ID排序
        String businessKey = BusinessKeyUtils.generate("u:" + normalizedUid, expIds);
        String paramJson = buildParamJson(normalizedUid, expIds);

        String taskId = taskSubmitService.submitTask(businessKey, paramJson, priority, maxRetry);
        return new SubmitResponse(taskId);
    }

    /**
     * 查询任务详情：状态、已重试次数、失败原因；成功时附带分桶结果。
     */
    @GetMapping("/{taskId}")
    public TaskDetailResponse detail(@PathVariable String taskId) {
        TaskEntity task = taskRepository.findById(taskId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "任务不存在: " + taskId));

        JsonNode result = null;
        if (task.getStatus() == TaskStatus.SUCCESS) {
            result = taskResultRepository.findByTaskId(taskId)
                    .map(TaskResultEntity::getResultJson)
                    .map(this::readJson)
                    .orElse(null);
        }
        return new TaskDetailResponse(
                task.getTaskId(),
                task.getStatus().name(),
                task.getRetryCount(),
                task.getMaxRetry(),
                task.getFailReason(),
                task.getCreateTime(),
                task.getUpdateTime(),
                result);
    }

    /**
     * 各状态任务数量统计（包含数量为 0 的状态）。
     */
    @GetMapping("/stat")
    public Map<String, Long> stat() {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (TaskStatus status : TaskStatus.values()) {
            counts.put(status.name(), taskRepository.countByStatus(status));
        }
        return counts;
    }

    private List<String> normalizeExpIds(List<String> expIdList) {
        Assert.notEmpty(expIdList, "expIdList 不能为空");
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        for (String expId : expIdList) {
            Assert.hasText(expId, "实验ID不能为空");
            String trimmed = expId.trim();
            Assert.isTrue(abTestBucketService.supports(trimmed), "不支持的实验ID: " + trimmed);
            normalized.add(trimmed);
        }
        return new ArrayList<>(normalized);
    }

    private String buildParamJson(String uid, List<String> expIds) {
        Map<String, Object> param = new LinkedHashMap<>();
        param.put("uid", uid);
        param.put("expIds", expIds);
        try {
            return objectMapper.writeValueAsString(param);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("任务参数序列化失败", e);
        }
    }

    private JsonNode readJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("任务结果解析失败", e);
        }
    }

    /**
     * 提交响应。
     */
    public record SubmitResponse(String taskId) {
    }

    /**
     * 任务详情响应。
     */
    public record TaskDetailResponse(String taskId,
                                     String status,
                                     int retryCount,
                                     int maxRetry,
                                     String failReason,
                                     LocalDateTime createTime,
                                     LocalDateTime updateTime,
                                     JsonNode result) {
    }
}
