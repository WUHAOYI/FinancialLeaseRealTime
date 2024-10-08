package com.why.bean.dws;
import com.why.bean.TransientSink;
import com.why.util.DateFormatUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
/**
 * Created by WHY on 2024/9/8.
 * Functions: 审批域信审经办粒度审批通过实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsAuditAuditManApprovalBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 信审经办 ID
    String auditManId;

    // 信审经办姓名
    String auditManName;

    // 申请项目数
    Long applyCount;

    // 申请金额
    BigDecimal applyAmount;

    // 批复金额
    BigDecimal replyAmount;

    // 通过时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    @TransientSink
    String approveTime;

    public Long getTs() {
        return DateFormatUtil.toTs(approveTime);
    }
}
