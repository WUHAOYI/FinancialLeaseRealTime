package com.why.bean;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by WHY on 2024/9/6.
 * Functions: 记录授信申请通过信息
 * 涉及到的表：credit_facility reply
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwdAuditApprovalBean {
    // 授信申请 ID
    String id;

    // 业务方向
    String leaseOrganization;

    // 申请人 ID
    String businessPartnerId;

    // 行业 ID
    String industryId;

    // 批复 ID
    String replyId;

    // 业务经办 ID
    String salesmanId;

    // TODO 信审经办 ID
    String auditManId;

    // 申请授信金额
    BigDecimal applyAmount;

    // TODO 批复金额
    BigDecimal replyAmount;

    // TODO 通过时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String approveTime;

    // TODO 批复时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String replyTime;

    // TODO 还款利率
    BigDecimal irr;

    // TODO 还款期数
    Long period;

}
