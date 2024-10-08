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
 * Functions: 审批域行业业务方向业务经办粒度审批拒绝实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DwsAuditIndLeaseOrgSalesmanRejectBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 业务方向
    String leaseOrganization;

    // 三级行业 ID
    String industry3Id;

    // 三级行业名称
    String industry3Name;

    // 二级行业 ID
    String industry2Id;

    // 二级行业名称
    String industry2Name;

    // 一级行业 ID
    String industry1Id;

    // 一级行业名称
    String industry1Name;

    // 业务经办 ID
    String salesmanId;

    // 业务经办姓名
    String salesmanName;

    // 三级部门 ID
    String department3Id;

    // 三级部门名称
    String department3Name;

    // 二级部门 ID
    String department2Id;

    // 二级部门名称
    String department2Name;

    // 一级部门 ID
    String department1Id;

    // 一级部门名称
    String department1Name;

    // 申请项目数
    Long applyCount;

    // 申请金额
    BigDecimal applyAmount;

    // 通过时间 yyyy-MM-dd HH:mm:ss.SSSSSS
    @TransientSink
    String rejectTime;

    public Long getTs() {
        return DateFormatUtil.toTs(rejectTime);
    }
}
