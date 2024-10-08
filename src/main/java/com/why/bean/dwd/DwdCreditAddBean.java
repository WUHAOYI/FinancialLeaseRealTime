package com.why.bean.dwd;

/**
 * Created by WHY on 2024/9/6.
 * Functions: 记录新增授信信息
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
public class DwdCreditAddBean {

    // 授信 ID
    String id;

    // 授信申请 ID
    String creditFacilityId;

    // 新增授信时间
    String addTime;

    // 申请金额
    BigDecimal applyAmount;

    // 批复金额
    BigDecimal replyAmount;

    // 授信金额
    BigDecimal creditAmount;

}
