#!/bin/bash

# 要删除的命名空间
NAMESPACE="FINANCIAL_LEASE_REALTIME"

for TABLE in "FINANCIAL_LEASE_REALTIME:dim_business_partner" "FINANCIAL_LEASE_REALTIME:dim_department" "FINANCIAL_LEASE_REALTIME:dim_employee" "FINANCIAL_LEASE_REALTIME:dim_industry"
do
  echo "禁用表: $TABLE"
  echo "disable '$TABLE'" | hbase shell -n
    
  echo "删除表: $TABLE"
  echo "drop '$TABLE'" | hbase shell -n
done

# 删除命名空间
echo "删除命名空间: $NAMESPACE"
echo "drop_namespace '$NAMESPACE'" | hbase shell -n
echo "create_namespace '$NAMESPACE'" | hbase shell -n
