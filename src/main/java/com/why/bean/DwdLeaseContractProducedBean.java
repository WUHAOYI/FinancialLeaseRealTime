package com.why.bean;

/**
 * Created by WHY on 2024/9/6.
 * Functions: 记录合同制作信息
 */
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwdLeaseContractProducedBean {

    // 合同 ID
    String id;

    // 授信 ID
    String creditId;

    // 授信申请 ID
    String creditFacilityId;

    // 完成合同制作时间
    String producedTime;

    // 申请金额
    BigDecimal applyAmount;

    // 批复金额
    BigDecimal replyAmount;

    // 授信金额
    BigDecimal creditAmount;

}
