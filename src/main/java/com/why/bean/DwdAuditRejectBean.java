package com.why.bean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
/**
 * Created by WHY on 2024/9/6.
 * Functions: 记录授信申请拒绝信息
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwdAuditRejectBean {
    // 授信申请 ID
    String id;

    // 业务方向
    String leaseOrganization;

    // 申请人 ID
    String businessPartnerId;

    // 行业 ID
    String industryId;

    // 业务经办 ID
    String salesmanId;

    // TODO 信审经办 ID
    String auditManId;

    // 申请授信金额
    BigDecimal applyAmount;

    // TODO 拒绝时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    String rejectTime;

}
