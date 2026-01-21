SELECT 
  STATUS_ST_DT,
  AGNT_ACCT_ID,
  SUM(HANDLED_FLAG) AS HANDLED, 
  SUM(HANDLED_TIME_WITH_ACW_SEC) AS OVERALL_HANDLED_TIME,
  QUE_NM,
  VERTICAL,
  SUBLOB,
  DEPARTMENT,
  VENDOR,
  SITE_NAME
FROM (
  SELECT 
    ----------------------------------------------------------------------
    -- Status date calculation
    ----------------------------------------------------------------------
    CASE 
      WHEN A.SEG_END_TS_UTC IS NOT NULL 
        THEN DATE(A.SEG_END_TS_UTC, 'US/Central')
      ELSE DATE(
        TIMESTAMP_ADD(
          A.SEG_START_TS_UTC, 
          INTERVAL A.TOT_TM_SEC_CNT SECOND
        ), 
        'US/Central'
      )
    END AS STATUS_ST_DT,

    A.AGNT_ACCT_ID,
    A.QUE_NM,
    QLKP.SVC_TIER_NM AS SUBLOB,
    QLKP.SVC_TIER_NM AS DEPARTMENT,
    QLKP.VERTICAL_NM AS VERTICAL,
    B.VENDOR,
    A.SITE_NM AS SITE_NAME,

    ----------------------------------------------------------------------
    -- Contact Start Time (SEG_START_TS_UTC + queue time)
    ----------------------------------------------------------------------
    DATETIME(
      TIMESTAMP_ADD(
        A.SEG_START_TS_UTC,
        INTERVAL A.QUE_TM_SEC_CNT SECOND
      ), 
      'US/Central'
    ) AS CONTACT_START_TIME,

    ----------------------------------------------------------------------
    -- Contact End Time
    ----------------------------------------------------------------------
    DATETIME(A.SEG_END_TS_UTC, 'US/Central') AS CONTACT_END_TIME,

    ----------------------------------------------------------------------
    -- Handled flag
    ----------------------------------------------------------------------
    CASE 
      WHEN A.TALK_TM_SEC_CNT > 0 THEN 1 
      ELSE 0 
    END AS HANDLED_FLAG,

    ----------------------------------------------------------------------
    -- Handled time per record (Handle Duration + ACW)
    ----------------------------------------------------------------------
    CASE 
      WHEN A.TALK_TM_SEC_CNT > 0 THEN
        TIMESTAMP_DIFF(
          DATETIME(A.SEG_END_TS_UTC, 'US/Central'),
          DATETIME(
            TIMESTAMP_ADD(
              A.SEG_START_TS_UTC,
              INTERVAL A.QUE_TM_SEC_CNT SECOND
            ), 
            'US/Central'
          ),
          SECOND
        )
        + COALESCE(A.ACW_TM_SEC_CNT, 0)
      ELSE 0
    END AS HANDLED_TIME_WITH_ACW_SEC

  FROM `wmt-edw-prod.WW_CUSTOMER_DL_VM.CS_CALL_DTL` A

  LEFT JOIN `wmt-edw-prod.WW_CUSTOMER_DL_VM.CS_QUE_LKP` QLKP 
    ON A.QUE_CD = QLKP.QUE_CD 
   AND A.SRC_APPLN_NM = QLKP.SRC_APPLN_NM

  LEFT JOIN `wmt-edw-prod.WW_CUSTOMER_DL_VM.CS_CALL_CNTR_SITE_LKP` D 
    ON A.SITE_CD = D.SITE_CD 
   AND A.SRC_APPLN_NM = D.SRC_APPLN_NM

  LEFT JOIN `wmt-cc-datasphere-prod.WFM_ADHOC.WFM_SITE_LKP` B 
    ON SAFE_CAST(A.SITE_CD AS FLOAT64) = B.SITE_CD
   AND CASE 
         WHEN A.SRC_APPLN_NM IN ('VCC-DRIVER', 'VCC - STORES') 
           THEN DATE(
                  TIMESTAMP_ADD(
                    A.SEG_START_TS_UTC, 
                    INTERVAL A.TOT_TM_SEC_CNT SECOND
                  ), 
                  'US/Central'
                ) 
         ELSE DATE(A.SEG_END_TS_UTC, 'US/Central')  
       END 
       BETWEEN SAFE_CAST(B.Start_Date AS DATE) 
           AND SAFE_CAST(B.End_Date AS DATE)

  WHERE A.QUE_NM IN (
    'Account Review Offline',
    'Account Review Team Email OB'
  )
)

WHERE STATUS_ST_DT BETWEEN '2025-02-01' AND CURRENT_DATE()

GROUP BY 
  STATUS_ST_DT,
  AGNT_ACCT_ID,
  QUE_NM,
  VERTICAL,
  SUBLOB,
  DEPARTMENT,
  VENDOR,
  SITE_NAME

ORDER BY 
  STATUS_ST_DT,
  AGNT_ACCT_ID,
  SITE_NAME
