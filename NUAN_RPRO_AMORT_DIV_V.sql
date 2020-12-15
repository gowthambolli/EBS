CREATE OR REPLACE VIEW APPS.NUAN_RPRO_AMORT_DIV_V (
    SOURCE,
    RPRO_RECORD_TYPE,
    OPERATING_UNIT,
    RC_ID,
    BSA,
    SALES_ORDER_NUMBER,
    ORDER_BOOKING_DATE,
    PROJECT_NUMBER,
    PROJECT_NAME,
    PROJECT_DESCRIPTION,
    PROJECT_MANAGER,
    TECHNICAL_ACCOUNT_MANAGER,
    PROJECT_OWNING_ORG,
    PROJECT_TYPE,
    PROGRAM,
    SALESREP_NAME,
    PROJECT_STATUS,
    PRACTICE,
    PS_OVERLAY,
    PLATFORM,
    PRODUCT_TYPE,
    ESTIMATED_GO_LIVE_DATE,
    ACTUAL_GO_LIVE_DATE,
    TERM,
    LATEST_BASELINE_PM_FCST_DATE,
    LATEST_BASELINE_PM_FCST_HRS,
    ORIGINAL_BUDGET_HRS,
    QE_BOOKING_AMT,
    QE_BOOKING_HRS,
    NXT_QE_BOOKING_AMT,
    NXT_QE_BOOKING_HRS,
    COMPLETE_BY_HRS,
    SOLD_TO_CUSTOMER_NAME,
    SOLD_TO_CUSTOMER_NUMBER,
    BILL_TO_CUSTOMER_NAME,
    BILL_TO_CUSTOMER_NUMBER,
    SHIP_TO_CUSTOMER_NAME,
    SHIP_TO_CUSTOMER_NUMBER,
    TOP_TASK_NUMBER,
    TASK_DESCRIPTION,
    POB_TEMPLATE,
    REV_ELEMENT,
    COMPANY,
    REGION,
    PRODUCT_FAMILY,
    DEPARTMENT,
    ACCOUNT,
    DEFERRED_ACCOUNT,
    REVENUE_ACCOUNT_NAME,
    PROD_GEN5,
    LINE_OF_BUSINESS,
    DIVISION,
    REVENUE_TYPE,
    REVENUE_METHOD_605,
    REVENUE_METHOD_606,
    ORIGINAL_ORDER_AMOUNT,
    SSP_ALLOCATION,
    FINAL_ALLOCATED_ORDER_AMOUNT,
    BOOKINGS_AFT_LST_PRD,
    TRANSACTIONAL_CURRENCY,
    FUNCTIONAL_CURRENCY,
    REV_START_DATE,
    REV_END_DATE,
    RELEASED,
    UNRELEASED,
    CFY_FCST_Q1,
    CFY_FCST_Q2,
    CFY_FCST_Q3,
    CFY_FCST_Q4,
    CFY1_FCST_Q1,
    CFY1_FCST_Q2,
    CFY1_FCST_Q3,
    CFY1_FCST_Q4,
    ITD_HRS,
    INCPNT_TO_STARTING_PERIOD,
    QE_1_BACKLOG,
    RQ_M1_ACTL,
    RQ_M2_ACTL,
    RQ_M3_ACTL,
    RQ_M4_ACTL,
    RQ_M5_ACTL,
    RQ_M6_ACTL,
    RQ_M1_ACTL_HR,
    RQ_M2_ACTL_HR,
    RQ_M3_ACTL_HR,
    RQ_M4_ACTL_HR,
    RQ_M5_ACTL_HR,
    RQ_M6_ACTL_HR,
    RQ_BACKLOG,
    RQ_M1_FCST,
    RQ_M2_FCST,
    RQ_M3_FCST,
    RQ_M4_FCST,
    RQ_M5_FCST,
    RQ_M6_FCST,
    RQ_M7_FCST,
    RQ_M8_FCST,
    RQ_M9_FCST,
    RQ_M10_FCST,
    RQ_M11_FCST,
    RQ_M12_FCST,
    RQ_M13_FCST,
    RQ_M14_FCST,
    RQ_M15_FCST,
    RQ_M16_FCST,
    RQ_M17_FCST,
    RQ_M18_FCST,
    RQ_M19_FCST,
    RQ_M20_FCST,
    RQ_M21_FCST,
    RQ_M22_FCST,
    RQ_M23_FCST,
    RQ_M24_FCST,
    RQ_M1_FCST_HR,
    RQ_M2_FCST_HR,
    RQ_M3_FCST_HR,
    RQ_M4_FCST_HR,
    RQ_M5_FCST_HR,
    RQ_M6_FCST_HR,
    RQ_M7_FCST_HR,
    RQ_M8_FCST_HR,
    RQ_M9_FCST_HR,
    RQ_M10_FCST_HR,
    RQ_M11_FCST_HR,
    RQ_M12_FCST_HR,
    RQ_M13_FCST_HR,
    RQ_M14_FCST_HR,
    RQ_M15_FCST_HR,
    RQ_M16_FCST_HR,
    RQ_M17_FCST_HR,
    RQ_M18_FCST_HR,
    RQ_M19_FCST_HR,
    RQ_M20_FCST_HR,
    RQ_M21_FCST_HR,
    RQ_M22_FCST_HR,
    RQ_M23_FCST_HR,
    RQ_M24_FCST_HR,
    FUTURE_FCST,
    FUTURE_FCST_HRS
) AS
    WITH
        FUNCTION QTR_FLAG (
            P_ACTUAL_PERIOD   DATE,
            P_SO_DATE         DATE,
            NXT_QE_FLAG       VARCHAR2
        ) RETURN NUMBER IS
            L_PRE_FISCAL_QTR_START_DATE   DATE;
            L_PRE_FISCAL_QTR_END_DATE     DATE;
            L_FISCAL_QTR_START_DATE       DATE;
            L_FISCAL_QTR_END_DATE         DATE;
            L_NXT_FISCAL_QTR_START_DATE   DATE;
            L_NXT_FISCAL_QTR_END_DATE     DATE;
        BEGIN
            SELECT
                TRUNC(
                    DECODE(
                        FISCAL_MONTH_OF_QUARTER,
                        'M1',
                        ADD_MONTHS(
                            DATE_KEY,
                            - 3
                        ),
                        'M2',
                        ADD_MONTHS(
                            DATE_KEY,
                            - 4
                        ),
                        'M3',
                        ADD_MONTHS(
                            DATE_KEY,
                            - 5
                        )
                    ),
                    'MONTH'
                ) PRE_FISCAL_QUARTER_START_DATE,
                DECODE(
                    FISCAL_MONTH_OF_QUARTER,
                    'M1',
                    ADD_MONTHS(
                        DATE_KEY,
                        - 1
                    ),
                    'M2',
                    ADD_MONTHS(
                        DATE_KEY,
                        - 2
                    ),
                    'M3',
                    ADD_MONTHS(
                        DATE_KEY,
                        - 3
                    )
                ) PRE_FISCAL_QUARTER_END_DATE,
                TRUNC(
                    DECODE(
                        FISCAL_MONTH_OF_QUARTER,
                        'M1',
                        DATE_KEY,
                        'M2',
                        ADD_MONTHS(
                            DATE_KEY,
                            - 1
                        ),
                        'M3',
                        ADD_MONTHS(
                            DATE_KEY,
                            - 2
                        )
                    ),
                    'MONTH'
                ) FISCAL_QUARTER_START_DATE,
                DECODE(
                    FISCAL_MONTH_OF_QUARTER,
                    'M1',
                    ADD_MONTHS(
                        DATE_KEY,
                        2
                    ),
                    'M2',
                    ADD_MONTHS(
                        DATE_KEY,
                        1
                    ),
                    'M3',
                    DATE_KEY
                ) FISCAL_QUARTER_END_DATE,
                TRUNC(
                    DECODE(
                        FISCAL_MONTH_OF_QUARTER,
                        'M1',
                        ADD_MONTHS(
                            DATE_KEY,
                            3
                        ),
                        'M2',
                        ADD_MONTHS(
                            DATE_KEY,
                            2
                        ),
                        'M3',
                        ADD_MONTHS(
                            DATE_KEY,
                        1
                        )
                    ),
                    'MONTH'
                ) NXT_FISCAL_QUARTER_START_DATE,
                DECODE(
                    FISCAL_MONTH_OF_QUARTER,
                    'M1',
                    ADD_MONTHS(
                        DATE_KEY,
                        5
                    ),
                    'M2',
                    ADD_MONTHS(
                        DATE_KEY,
                        4
                    ),
                    'M3',
                    ADD_MONTHS(
                        DATE_KEY,
                        3
                    )
                ) NXT_FISCAL_QUARTER_END_DATE
            INTO
                L_PRE_FISCAL_QTR_START_DATE,
                L_PRE_FISCAL_QTR_END_DATE,
                L_FISCAL_QTR_START_DATE,
                L_FISCAL_QTR_END_DATE,
                L_NXT_FISCAL_QTR_START_DATE,
                L_NXT_FISCAL_QTR_END_DATE
            FROM
                XXNUAN.NUAN_BI_DATE_DIM_T T
            WHERE
                DATE_KEY = P_ACTUAL_PERIOD;
            IF NXT_QE_FLAG = 'NXT' THEN
                IF P_SO_DATE >= L_FISCAL_QTR_START_DATE AND P_SO_DATE <= L_FISCAL_QTR_END_DATE THEN
                    RETURN 1;
                ELSE
                    RETURN 0;
                END IF;
            ELSIF NXT_QE_FLAG = 'QE' THEN
                IF P_SO_DATE >= L_PRE_FISCAL_QTR_START_DATE AND P_SO_DATE <= L_PRE_FISCAL_QTR_END_DATE THEN
                    RETURN 1;
                ELSE
                    RETURN 0;
                END IF;        
            ELSIF NXT_QE_FLAG = 'AFT' THEN
                IF P_SO_DATE > L_FISCAL_QTR_START_DATE-1 THEN
                    RETURN 1;
                ELSE
                    RETURN 0;
                END IF;            
            END IF;
        END;
    SELECT /*+ WITH_PLSQL */
        SOURCE,
        RPRO_RECORD_TYPE,
        OPERATING_UNIT,
        RC_ID,
        BSA,
        SALES_ORDER_NUMBER,
        ORDER_BOOKING_DATE,
        PROJECT_NUMBER,
        PROJECT_NAME,
        PROJECT_DESCRIPTION,
        PROJECT_MANAGER,
        TECHNICAL_ACCOUNT_MANAGER,
        PROJECT_OWNING_ORG,
        PROJECT_TYPE,
        PROGRAM,
        SALESREP_NAME,
        PROJECT_STATUS,
        PRACTICE,
        PS_OVERLAY,
        PLATFORM,
        PRODUCT_TYPE,
        ESTIMATED_GO_LIVE_DATE,
        ACTUAL_GO_LIVE_DATE,
        TERM,
        LATEST_BASELINE_PM_FCST_DATE,
        LATEST_BASELINE_PM_FCST_HRS,
        ORIGINAL_BUDGET_HRS * NVL(
            OBH_RTR,
            1
        ) ORIGINAL_BUDGET_HRS,
        DECODE(
            QTR_FLAG(
                L_ACTUAL_PERIOD,
                ORDER_BOOKING_DATE,
                'QE'
            ),
            1,
            ORIGINAL_ORDER_AMOUNT,
            0
        ) QE_BOOKING_AMT,
        QE_BOOKING_HRS * NVL(
            QBH_RTR,
            1
        ) QE_BOOKING_HRS,
        DECODE(
            QTR_FLAG(
                L_ACTUAL_PERIOD,
                ORDER_BOOKING_DATE,
                'NXT'
            ),
            1,
            ORIGINAL_ORDER_AMOUNT,0
        ) NXT_QE_BOOKING_AMT,
        NXT_QE_BOOKING_HRS * NVL(
            NQBH_RTR,
            1
        ) NXT_QE_BOOKING_HRS,
        COMPLETE_BY_HRS,
        SOLD_TO_CUSTOMER_NAME,
        SOLD_TO_CUSTOMER_NUMBER,
        BILL_TO_CUSTOMER_NAME,
        BILL_TO_CUSTOMER_NUMBER,
        SHIP_TO_CUSTOMER_NAME,
        SHIP_TO_CUSTOMER_NUMBER,
        TOP_TASK_NUMBER,
        TASK_DESCRIPTION,
        POB_TEMPLATE,
        REV_ELEMENT,
        COMPANY,
        REGION,
        PRODUCT_FAMILY,
        DEPARTMENT,
        ACCOUNT,
        DEFERRED_ACCOUNT,
        REVENUE_ACCOUNT_NAME,
        PROD_GEN5,
        LINE_OF_BUSINESS,
        DIVISION,
        REVENUE_TYPE,
        REVENUE_METHOD_605,
        REVENUE_METHOD_606,
        ORIGINAL_ORDER_AMOUNT,
        SSP_ALLOCATION,
        FINAL_ALLOCATED_ORDER_AMOUNT,
        CASE
            WHEN ORDER_BOOKING_DATE > L_ACTUAL_PERIOD THEN
                ORIGINAL_ORDER_AMOUNT
            ELSE
                0
        END BOOKINGS_AFT_LST_PRD,
        TRANSACTIONAL_CURRENCY,
        FUNCTIONAL_CURRENCY,
        REV_START_DATE,
        REV_END_DATE,
        RELEASED,
        UNRELEASED,
        CFY_FCST_Q1,
        CFY_FCST_Q2,
        CFY_FCST_Q3,
        CFY_FCST_Q4,
        CFY1_FCST_Q1,
        CFY1_FCST_Q2,
        CFY1_FCST_Q3,
        CFY1_FCST_Q4,
        ITD_HRS,
        INCPNT_TO_STARTING_PERIOD,
        QE_1_BACKLOG - DECODE(
            QTR_FLAG(
                L_ACTUAL_PERIOD,
                ORDER_BOOKING_DATE,
                'AFT'
            ),
            1,
            FINAL_ALLOCATED_ORDER_AMOUNT,0
        ) QE_1_BACKLOG,
        RQ_M1_ACTL,
        RQ_M2_ACTL,
        RQ_M3_ACTL,
        RQ_M4_ACTL,
        RQ_M5_ACTL,
        RQ_M6_ACTL,
        RQ_M1_ACTL_HR,
        RQ_M2_ACTL_HR,
        RQ_M3_ACTL_HR,
        RQ_M4_ACTL_HR,
        RQ_M5_ACTL_HR,
        RQ_M6_ACTL_HR,
        CASE
            WHEN ORDER_BOOKING_DATE > L_ACTUAL_PERIOD THEN
                RQ_BACKLOG - FINAL_ALLOCATED_ORDER_AMOUNT
            ELSE
                RQ_BACKLOG
        END RQ_BACKLOG,
        RQ_M1_FCST,
        RQ_M2_FCST,
        RQ_M3_FCST,
        RQ_M4_FCST,
        RQ_M5_FCST,
        RQ_M6_FCST,
        RQ_M7_FCST,
        RQ_M8_FCST,
        RQ_M9_FCST,
        RQ_M10_FCST,
        RQ_M11_FCST,
        RQ_M12_FCST,
        RQ_M13_FCST,
        RQ_M14_FCST,
        RQ_M15_FCST,
        RQ_M16_FCST,
        RQ_M17_FCST,
        RQ_M18_FCST,
        RQ_M19_FCST,
        RQ_M20_FCST,
        RQ_M21_FCST,
        RQ_M22_FCST,
        RQ_M23_FCST,
        RQ_M24_FCST,
        RQ_M1_FCST_HR,
        RQ_M2_FCST_HR,
        RQ_M3_FCST_HR,
        RQ_M4_FCST_HR,
        RQ_M5_FCST_HR,
        RQ_M6_FCST_HR,
        RQ_M7_FCST_HR,
        RQ_M8_FCST_HR,
        RQ_M9_FCST_HR,
        RQ_M10_FCST_HR,
        RQ_M11_FCST_HR,
        RQ_M12_FCST_HR,
        RQ_M13_FCST_HR,
        RQ_M14_FCST_HR,
        RQ_M15_FCST_HR,
        RQ_M16_FCST_HR,
        RQ_M17_FCST_HR,
        RQ_M18_FCST_HR,
        RQ_M19_FCST_HR,
        RQ_M20_FCST_HR,
        RQ_M21_FCST_HR,
        RQ_M22_FCST_HR,
        RQ_M23_FCST_HR,
        RQ_M24_FCST_HR,
        FUTURE_FCST,
        FUTURE_FCST_HRS
    FROM
        (
            SELECT
/*
REM $Header: $
REM ================================================================================
REM  Copyright (C)  2015  Nuance Communications,Inc. Pune India                    |
REM                        All rights reserved.                                    |
REM ================================================================================
REM   View  Name    : NUAN_RPRO_AMORT_DIV_V.sql                                      |
REM   Purpose       : Use in divisional version of revenue backlog report          |
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++|
REM   VERSION  DATE          AUTHOR          DESCRIPTION                           |
REM   -----------------------------------------------------------------------------|
REM   1.0      12-DEC-2020   Pradeep Chavan  RPRO - Initial Version                |     
REM ===============================================================================|
*/SOURCE,
                ATR25,
                ATR60,
                ATR1              RPRO_RECORD_TYPE,
                OPERATING_UNIT,
                RC_ID,
                QUOTE_NUM         BSA,
                SALES_ORDER_NUMBER,
                DOC_DATE          ORDER_BOOKING_DATE,
                PROJECT_NUMBER,
                PROJECT_NAME,
                REGEXP_REPLACE(
                    PROJECT_DESCRIPTION,
                    '[^0-9A-Za-z]',
                    ' '
                ) PROJECT_DESCRIPTION,
                PROJECT_MANAGER,
                TECHNICAL_ACCOUNT_MANAGER,
                PROJECT_OWNING_ORG,
                PROJECT_TYPE,
                PROGRAM,
                SALESREP_NAME,
                PROJECT_STATUS,
                PRACTICE,
                PS_OVERLAY,
                PLATFORM,
                PRODUCT_TYPE,
                ESTIMATED_GO_LIVE_DATE,
                ACTUAL_GO_LIVE_DATE,
                TERM,
                LATEST_BASELINE_PM_FCST_DATE,
                LATEST_BASELINE_PM_FCST_HRS,
                ORIGINAL_BUDGET_HRS,
                RATIO_TO_REPORT(ORIGINAL_BUDGET_HRS) OVER(
                    PARTITION BY RC_ID,
                                 PROJECT_NUMBER
                ) AS OBH_RTR,
                SUM(QE_BOOKING_AMT) QE_BOOKING_AMT,
                1 QBA_RTR,
                QE_BOOKING_HRS,
                RATIO_TO_REPORT(QE_BOOKING_HRS) OVER(
                    PARTITION BY RC_ID,
                                 PROJECT_NUMBER
                ) AS QBH_RTR,
                SUM(NXT_QE_BOOKING_AMT) NXT_QE_BOOKING_AMT,
                1 NQBA_RTR,
                NXT_QE_BOOKING_HRS,
                RATIO_TO_REPORT(NXT_QE_BOOKING_HRS) OVER(
                    PARTITION BY RC_ID,
                                 PROJECT_NUMBER
                ) AS NQBH_RTR,
                DECODE(
                    SUM(COMPLETE_BY_HRS),
                    NULL,
                    NULL,
                    SUM(COMPLETE_BY_HRS)
                    || '%'
                ) COMPLETE_BY_HRS,
                SOLD_TO_CUSTOMER_NAME,
                SOLD_TO_CUSTOMER_NUMBER,
                BILL_TO_CUSTOMER_NAME,
                BILL_TO_CUSTOMER_NUMBER,
                SHIP_TO_CUSTOMER_NAME,
                SHIP_TO_CUSTOMER_NUMBER,
                DECODE(
                    TOP_TASK_NUMBER,
                    'USD',
                    NULL,
                    TOP_TASK_NUMBER
                ) TOP_TASK_NUMBER,
                TASK_DESCRIPTION,
                POB_TEMPLATE,
                REV_ELEMENT,
                COMPANY,
                REGION,
                PRODUCT_FAMILY,
                DEPARTMENT,
                ACCOUNT,
                DEFERRED_ACCOUNT,
                REVENUE_ACCOUNT_NAME,
                PROD_GEN5,
                PROD_GEN2         LINE_OF_BUSINESS,
                DIVISION,
                ACCOUNT_ROLLUP1   REVENUE_TYPE,
                REVENUE_METHOD_605,
                REVENUE_METHOD_606,
                SUM(ORIGINAL_ORDER_AMOUNT) ORIGINAL_ORDER_AMOUNT,
                SUM(SSP_ALLOCATION) SSP_ALLOCATION,
                SUM(FINAL_ALLOCATED_ORDER_AMOUNT) FINAL_ALLOCATED_ORDER_AMOUNT,
                SUM(BOOKINGS_AFT_LST_PRD) BOOKINGS_AFT_LST_PRD,
                SUM(RQ_ENDING_TOTAL) RQ_ENDING_TOTAL,
                TRANSACTIONAL_CURRENCY,
                FUNCTIONAL_CURRENCY,
                REV_START_DATE,
                REV_END_DATE,
                SUM(RELEASED) RELEASED,
                SUM(UNRELEASED) UNRELEASED,
                SUM(CFY_FCST_Q1) CFY_FCST_Q1,
                SUM(CFY_FCST_Q2) CFY_FCST_Q2,
                SUM(CFY_FCST_Q3) CFY_FCST_Q3,
                SUM(CFY_FCST_Q4) CFY_FCST_Q4,
                SUM(CFY1_FCST_Q1) CFY1_FCST_Q1,
                SUM(CFY1_FCST_Q2) CFY1_FCST_Q2,
                SUM(CFY1_FCST_Q3) CFY1_FCST_Q3,
                SUM(CFY1_FCST_Q4) CFY1_FCST_Q4,
                SUM(ITD_HRS) ITD_HRS,
                SUM(RQ_1_BACKLOG) INCPNT_TO_STARTING_PERIOD,
                SUM(QE_1_BACKLOG) QE_1_BACKLOG,
                SUM(RQ_M1_ACTL) RQ_M1_ACTL,
                SUM(RQ_M2_ACTL) RQ_M2_ACTL,
                SUM(RQ_M3_ACTL) RQ_M3_ACTL,
                SUM(RQ_M4_ACTL) RQ_M4_ACTL,
                SUM(RQ_M5_ACTL) RQ_M5_ACTL,
                SUM(RQ_M6_ACTL) RQ_M6_ACTL,
                SUM(RQ_M1_ACTL_HR) RQ_M1_ACTL_HR,
                SUM(RQ_M2_ACTL_HR) RQ_M2_ACTL_HR,
                SUM(RQ_M3_ACTL_HR) RQ_M3_ACTL_HR,
                SUM(RQ_M4_ACTL_HR) RQ_M4_ACTL_HR,
                SUM(RQ_M5_ACTL_HR) RQ_M5_ACTL_HR,
                SUM(RQ_M6_ACTL_HR) RQ_M6_ACTL_HR,
                SUM(RQ_BACKLOG) RQ_BACKLOG,
                SUM(RQ_M1_FCST) RQ_M1_FCST,
                SUM(RQ_M2_FCST) RQ_M2_FCST,
                SUM(RQ_M3_FCST) RQ_M3_FCST,
                SUM(RQ_M4_FCST) RQ_M4_FCST,
                SUM(RQ_M5_FCST) RQ_M5_FCST,
                SUM(RQ_M6_FCST) RQ_M6_FCST,
                SUM(RQ_M7_FCST) RQ_M7_FCST,
                SUM(RQ_M8_FCST) RQ_M8_FCST,
                SUM(RQ_M9_FCST) RQ_M9_FCST,
                SUM(RQ_M10_FCST) RQ_M10_FCST,
                SUM(RQ_M11_FCST) RQ_M11_FCST,
                SUM(RQ_M12_FCST) RQ_M12_FCST,
                SUM(RQ_M13_FCST) RQ_M13_FCST,
                SUM(RQ_M14_FCST) RQ_M14_FCST,
                SUM(RQ_M15_FCST) RQ_M15_FCST,
                SUM(RQ_M16_FCST) RQ_M16_FCST,
                SUM(RQ_M17_FCST) RQ_M17_FCST,
                SUM(RQ_M18_FCST) RQ_M18_FCST,
                SUM(RQ_M19_FCST) RQ_M19_FCST,
                SUM(RQ_M20_FCST) RQ_M20_FCST,
                SUM(RQ_M21_FCST) RQ_M21_FCST,
                SUM(RQ_M22_FCST) RQ_M22_FCST,
                SUM(RQ_M23_FCST) RQ_M23_FCST,
                SUM(RQ_M24_FCST) RQ_M24_FCST,
                SUM(RQ_M1_FCST_HR) RQ_M1_FCST_HR,
                SUM(RQ_M2_FCST_HR) RQ_M2_FCST_HR,
                SUM(RQ_M3_FCST_HR) RQ_M3_FCST_HR,
                SUM(RQ_M4_FCST_HR) RQ_M4_FCST_HR,
                SUM(RQ_M5_FCST_HR) RQ_M5_FCST_HR,
                SUM(RQ_M6_FCST_HR) RQ_M6_FCST_HR,
                SUM(RQ_M7_FCST_HR) RQ_M7_FCST_HR,
                SUM(RQ_M8_FCST_HR) RQ_M8_FCST_HR,
                SUM(RQ_M9_FCST_HR) RQ_M9_FCST_HR,
                SUM(RQ_M10_FCST_HR) RQ_M10_FCST_HR,
                SUM(RQ_M11_FCST_HR) RQ_M11_FCST_HR,
                SUM(RQ_M12_FCST_HR) RQ_M12_FCST_HR,
                SUM(RQ_M13_FCST_HR) RQ_M13_FCST_HR,
                SUM(RQ_M14_FCST_HR) RQ_M14_FCST_HR,
                SUM(RQ_M15_FCST_HR) RQ_M15_FCST_HR,
                SUM(RQ_M16_FCST_HR) RQ_M16_FCST_HR,
                SUM(RQ_M17_FCST_HR) RQ_M17_FCST_HR,
                SUM(RQ_M18_FCST_HR) RQ_M18_FCST_HR,
                SUM(RQ_M19_FCST_HR) RQ_M19_FCST_HR,
                SUM(RQ_M20_FCST_HR) RQ_M20_FCST_HR,
                SUM(RQ_M21_FCST_HR) RQ_M21_FCST_HR,
                SUM(RQ_M22_FCST_HR) RQ_M22_FCST_HR,
                SUM(RQ_M23_FCST_HR) RQ_M23_FCST_HR,
                SUM(RQ_M24_FCST_HR) RQ_M24_FCST_HR,
                ROUND(
                    (SUM(FUTURE_FCST)),
                    2
                ) FUTURE_FCST,
                SUM(FUTURE_FCST_HRS) FUTURE_FCST_HRS,
                L_START_PERIOD,
                L_ACTUAL_PERIOD
            FROM
                (
                    WITH LOB_ITD_HRS AS (
                        SELECT
                            PROJECT_ID   LOB_PROJECT_ID,
                            ATR25        LOB_ATR25,
                            LISTAGG(TOP_TASK_NUMBER,
                                    ',') WITHIN GROUP(
                                ORDER BY
                                    TOP_TASK_NUMBER
                            ) LIST_OF_TASK,
                            SUM(ITD_HRS) LOB_ITD_HRS,
                            SUM(RQ_M1_ACTL_HR) LOB_RQ_M1_ACTL_HR,
                            SUM(RQ_M2_ACTL_HR) LOB_RQ_M2_ACTL_HR,
                            SUM(RQ_M3_ACTL_HR) LOB_RQ_M3_ACTL_HR,
                            SUM(RQ_M4_ACTL_HR) LOB_RQ_M4_ACTL_HR,
                            SUM(RQ_M5_ACTL_HR) LOB_RQ_M5_ACTL_HR,
                            SUM(RQ_M6_ACTL_HR) LOB_RQ_M6_ACTL_HR,
                            SUM(RQ_M1_FCST_HR) LOB_RQ_M1_FCST_HR,
                            SUM(RQ_M2_FCST_HR) LOB_RQ_M2_FCST_HR,
                            SUM(RQ_M3_FCST_HR) LOB_RQ_M3_FCST_HR,
                            SUM(RQ_M4_FCST_HR) LOB_RQ_M4_FCST_HR,
                            SUM(RQ_M5_FCST_HR) LOB_RQ_M5_FCST_HR,
                            SUM(RQ_M6_FCST_HR) LOB_RQ_M6_FCST_HR,
                            SUM(RQ_M7_FCST_HR) LOB_RQ_M7_FCST_HR,
                            SUM(RQ_M8_FCST_HR) LOB_RQ_M8_FCST_HR,
                            SUM(RQ_M9_FCST_HR) LOB_RQ_M9_FCST_HR,
                            SUM(RQ_M10_FCST_HR) LOB_RQ_M10_FCST_HR,
                            SUM(RQ_M11_FCST_HR) LOB_RQ_M11_FCST_HR,
                            SUM(RQ_M12_FCST_HR) LOB_RQ_M12_FCST_HR,
                            SUM(RQ_M13_FCST_HR) LOB_RQ_M13_FCST_HR,
                            SUM(RQ_M14_FCST_HR) LOB_RQ_M14_FCST_HR,
                            SUM(RQ_M15_FCST_HR) LOB_RQ_M15_FCST_HR,
                            SUM(RQ_M16_FCST_HR) LOB_RQ_M16_FCST_HR,
                            SUM(RQ_M17_FCST_HR) LOB_RQ_M17_FCST_HR,
                            SUM(RQ_M18_FCST_HR) LOB_RQ_M18_FCST_HR,
                            SUM(RQ_M19_FCST_HR) LOB_RQ_M19_FCST_HR,
                            SUM(RQ_M20_FCST_HR) LOB_RQ_M20_FCST_HR,
                            SUM(RQ_M21_FCST_HR) LOB_RQ_M21_FCST_HR,
                            SUM(RQ_M22_FCST_HR) LOB_RQ_M22_FCST_HR,
                            SUM(RQ_M23_FCST_HR) LOB_RQ_M23_FCST_HR,
                            SUM(RQ_M24_FCST_HR) LOB_RQ_M24_FCST_HR,
                            SUM(FUTURE_FCST_HRS) LOB_FUTURE_FCST_HRS,
                            SUM(LATEST_BASELINE_PM_FOR_HRS) LOB_LATEST_BASELINE_PM_FOR_HRS
                        FROM
                            NUAN_RPRO_AMOR_HRS_MATRIX_GT
                        WHERE
                            REQUEST_ID = NVL(
                                FND_GLOBAL.CONC_PRIORITY_REQUEST,
                                FND_GLOBAL.CONC_REQUEST_ID
                            )
                        GROUP BY
                            PROJECT_ID,
                            ATR25
                    )
                    SELECT
                        MNT.ATR25,
                        WATER.ATR60,
                        'REVPRO' SOURCE,
                        WATER.ATR1,
                        MNT.RC_ID,
                        MNT.SO_NUM                          SALES_ORDER_NUMBER,
                        HIST.PROJECT_NUMBER                 PROJECT_NUMBER,
                        NULL MS_SOURCE_ID,
                        NULL MS_SOURCE_NAME,
                        REPLACE(
                            REPLACE(
                                LTRIM(
                                    RTRIM(
                                        HIST.PROJECT_DESCRIPTION
                                    )
                                ),
                                '',
                                ''
                            ),
                            CHR(10),
                            ''
                        ) PROJECT_DESCRIPTION,
                        HIST.PROJECT_NAME                   PROJECT_NAME,
                        HIST.PROJECT_MANAGER                PROJECT_MANAGER,
                        HIST.TECHNICAL_ACCOUNT_MANAGER,
                        NVL(
                            WATER.BUSINESS_UNIT,
                            HIST.OPERATING_UNIT
                        ) OPERATING_UNIT,
                        HIST.PROJECT_OWN_ORG                PROJECT_OWNING_ORG,
                        HIST.PROJECT_TYPE                   PROJECT_TYPE,
                        HIST.PROGRAM                        PROGRAM,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.SALES_REP_NAME
                            ELSE
                                HIST.SALES_PERSON
                        END SALESREP_NAME,
                        WATER.F_CUR                         FUNCTIONAL_CURRENCY,
                        WATER.CURR                          TRANSACTIONAL_CURRENCY,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.CSTMR_NM
                            ELSE
                                HIST.SOLD_TO_CUSTOMER_NAME
                        END SOLD_TO_CUSTOMER_NAME,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.CUST_NUM
                            ELSE
                                HIST.SOLD_TO_CUSTOMER_NUMBER
                        END SOLD_TO_CUSTOMER_NUMBER,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.ATR2
                            ELSE
                                HIST.BILL_TO_CUSTOMER_NAME
                        END BILL_TO_CUSTOMER_NAME,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.ATR3
                            ELSE
                                HIST.BILL_TO_CUSTOMER_NUMBER
                        END BILL_TO_CUSTOMER_NUMBER,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.ATR13
                            ELSE
                                HIST.SHIP_TO_CUSTOMER_NAME
                        END SHIP_TO_CUSTOMER_NAME,
                        CASE
                            WHEN WATER.ATR16 IS NULL THEN
                                WATER.ATR12
                            ELSE
                                HIST.SHIP_TO_CUSTOMER_NUMBER
                        END SHIP_TO_CUSTOMER_NUMBER,
                        HIST.PROJECT_STATUS                 PROJECT_STATUS,
                        MNT.REVENUE_ELEMENT                 REV_ELEMENT,
                        MNT.POB_TMPL_NAME                   POB_TEMPLATE,
                        MNT.TOP_TASK_NUMBER,
                        NUAN_RPRO_R_MATRICS_PKG.GET_TASK_DESC(
                            MNT.TOP_TASK_ID
                        ) TASK_DESCRIPTION,
                        SUBSTR(
                            MNT.REV_SEGMENTS,
                            1,
                            2
                        ) COMPANY,
                        SUBSTR(
                            MNT.REV_SEGMENTS,
                            4,
                            3
                        ) DEPARTMENT,
                        SUBSTR(
                            MNT.REV_SEGMENTS,
                            8,
                            6
                        ) ACCOUNT,
                        SUBSTR(
                            MNT.REV_SEGMENTS,
                            15,
                            3
                        ) PRODUCT_FAMILY,
                        SUBSTR(
                            MNT.REV_SEGMENTS,
                            19,
                            2
                        ) REGION,
                        SUBSTR(
                            MNT.DEF_SEGMENTS,
                            8,
                            6
                        ) DEFERRED_ACCOUNT,
                        WATER.ATR8                          DIVISION,
                        HIST.PRACTICE                       PRACTICE,
                        HIST.PS_OVERLAY                     PS_OVERLAY,
                        HIST.PLATFORM,
                        HIST.REVENUE_METHOD_605             REVENUE_METHOD_605,
                        HIST.REVENUE_METHOD_606             REVENUE_METHOD_606,
                        HIST.PRODUCT_TYPE,
                        WATER.DATE1                         ESTIMATED_GO_LIVE_DATE,
                        WATER.DATE2                         ACTUAL_GO_LIVE_DATE,
                        WATER.TERM                          TERM,
                        NPD.PROD_GEN2,
                        NVL(
                            MNT.ORDER_AMOUNT,
                            0
                        ) ORIGINAL_ORDER_AMOUNT,
                        NVL(
                            MNT.SSP_ALLOCATION,
                            0
                        ) SSP_ALLOCATION,
                        NVL(
                            MNT.FINAL_ALLOCATED_ORD_AMT,
                            0
                        ) FINAL_ALLOCATED_ORDER_AMOUNT,
                        MNT.BOOKINGS_AFT_LST_PRD,
                        NVL(
                            LOB.LOB_ITD_HRS,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) ITD_HRS,
                        REV_START_DATE                      REV_START_DATE,
                        REV_END_DATE                        REV_END_DATE,
                        HIST.LATEST_BASELINE_PM_FCST_DATE   LATEST_BASELINE_PM_FCST_DATE,
                        DECODE(
                            MNT.TOP_TASK_NUMBER,
                            'Consulting Top Task',
                            NVL(
                                LOB_LATEST_BASELINE_PM_FOR_HRS,
                                0
                            ),
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) LATEST_BASELINE_PM_FCST_HRS,
                        CASE
                            WHEN HRS.TOP_TASK_NUMBER = MNT.TOP_TASK_NUMBER THEN
                                HIST.ORIGINAL_BUDGET_HRS
                            ELSE
                                0
                        END ORIGINAL_BUDGET_HRS,
                        NVL(
                            MNT.QE_BOOKING_AMT,
                            0
                        ) QE_BOOKING_AMT,
                        NVL(
                            MNT.NXT_QE_BOOKING_AMT,
                            0
                        ) NXT_QE_BOOKING_AMT,
                        CASE
                            WHEN HRS.TOP_TASK_NUMBER = MNT.TOP_TASK_NUMBER THEN
                                NVL(
                                    MNT.QE_BOOKING_HRS,
                                    0
                                )
                            ELSE
                                0
                        END QE_BOOKING_HRS,
                        CASE
                            WHEN HRS.TOP_TASK_NUMBER = MNT.TOP_TASK_NUMBER THEN
                                MNT.NXT_QE_BOOKING_HRS
                            ELSE
                                0
                        END NXT_QE_BOOKING_HRS,
                        ROUND(
                            (DECODE(
                                MNT.POB_TMPL_NAME,
                                'UPON COMPLETION BY PERCENT',
                                NVL(
                                    (DECODE(
                                        LOB_LATEST_BASELINE_PM_FOR_HRS,
                                        0,
                                        0,
                                        (((LOB.LOB_ITD_HRS + LOB.LOB_RQ_M1_ACTL_HR + LOB.LOB_RQ_M2_ACTL_HR + LOB.LOB_RQ_M3_ACTL_HR
                                        + LOB.LOB_RQ_M4_ACTL_HR + LOB.LOB_RQ_M5_ACTL_HR + LOB.LOB_RQ_M6_ACTL_HR) /(NVL(
                                            LOB_LATEST_BASELINE_PM_FOR_HRS,
                                            0
                                        ))))
                                    )) * 100,
                                    0
                                ),
                                NULL
                            )),
                            2
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) COMPLETE_BY_HRS,
                        NVL(
                            MNT.QE_1_REV_REC_ITD,
                            0
                        ) QE_1_REV_REC_ITD,
                        NVL(
                            MNT.QE_1_BACKLOG,
                            0
                        ) QE_1_BACKLOG,
                        NVL(
                            RELEASED_AMT,
                            0
                        ) RELEASED,
                        NVL(
                            MNT.FINAL_ALLOCATED_ORD_AMT,
                            0
                        ) - NVL(
                            RELEASED_AMT,
                            0
                        ) UNRELEASED,
                        MNT.RQ_M1_ACTL,
                        MNT.RQ_M2_ACTL,
                        MNT.RQ_M3_ACTL,
                        MNT.RQ_M4_ACTL,
                        MNT.RQ_M5_ACTL,
                        MNT.RQ_M6_ACTL,
                        NVL(
                            LOB.LOB_RQ_M1_ACTL_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M1_ACTL_HR,
                        NVL(
                            LOB.LOB_RQ_M2_ACTL_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M2_ACTL_HR,
                        NVL(
                            LOB.LOB_RQ_M3_ACTL_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M3_ACTL_HR,
                        NVL(
                            LOB.LOB_RQ_M4_ACTL_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M4_ACTL_HR,
                        NVL(
                            LOB.LOB_RQ_M5_ACTL_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M5_ACTL_HR,
                        NVL(
                            LOB.LOB_RQ_M6_ACTL_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M6_ACTL_HR,
                        NVL(
                            MNT.RQ_BACKLOG,
                            0
                        ) RQ_BACKLOG,
                        MNT.RQ_M1_FCST,
                        MNT.RQ_M2_FCST,
                        MNT.RQ_M3_FCST,
                        MNT.RQ_M4_FCST,
                        MNT.RQ_M5_FCST,
                        MNT.RQ_M6_FCST,
                        MNT.RQ_M7_FCST,
                        MNT.RQ_M8_FCST,
                        MNT.RQ_M9_FCST,
                        MNT.RQ_M10_FCST,
                        MNT.RQ_M11_FCST,
                        MNT.RQ_M12_FCST,
                        MNT.RQ_M13_FCST,
                        MNT.RQ_M14_FCST,
                        MNT.RQ_M15_FCST,
                        MNT.RQ_M16_FCST,
                        MNT.RQ_M17_FCST,
                        MNT.RQ_M18_FCST,
                        MNT.RQ_M19_FCST,
                        MNT.RQ_M20_FCST,
                        MNT.RQ_M21_FCST,
                        MNT.RQ_M22_FCST,
                        MNT.RQ_M23_FCST,
                        MNT.RQ_M24_FCST,
                        NVL(
                            LOB.LOB_RQ_M1_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M1_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M2_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M2_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M3_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M3_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M4_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M4_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M5_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M5_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M6_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M6_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M7_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M7_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M8_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M8_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M9_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M9_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M10_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M10_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M11_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M11_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M12_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M12_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M13_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M13_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M14_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M14_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M15_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M15_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M16_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M16_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M17_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M17_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M18_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M18_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M19_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M19_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M20_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M20_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M21_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M21_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M22_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M22_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M23_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M23_FCST_HR,
                        NVL(
                            LOB.LOB_RQ_M24_FCST_HR,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) RQ_M24_FCST_HR,
                        NVL(
                            LOB.LOB_FUTURE_FCST_HRS,
                            0
                        ) / COUNT(1) OVER(
                            PARTITION BY MNT.PROJECT_ID,
                                         MNT.RC_ID,
                                         MNT.ATR25
                        ) FUTURE_FCST_HRS,
                        MNT.RQ_ENDING_TOTAL,
                        NVL(
                            MNT.CFY_FCST_Q1,
                            0
                        ) CFY_FCST_Q1,
                        NVL(
                            MNT.CFY_FCST_Q2,
                            0
                        ) CFY_FCST_Q2,
                        NVL(
                            MNT.CFY_FCST_Q3,
                            0
                        ) CFY_FCST_Q3,
                        NVL(
                            MNT.CFY_FCST_Q4,
                            0
                        ) CFY_FCST_Q4,
                        NVL(
                            MNT.CFY1_FCST_Q1,
                            0
                        ) CFY1_FCST_Q1,
                        NVL(
                            MNT.CFY1_FCST_Q2,
                            0
                        ) CFY1_FCST_Q2,
                        NVL(
                            MNT.CFY1_FCST_Q3,
                            0
                        ) CFY1_FCST_Q3,
                        NVL(
                            MNT.CFY1_FCST_Q4,
                            0
                        ) CFY1_FCST_Q4,
                        NVL(
                            MNT.FUTURE_FCST,
                            0
                        ) FUTURE_FCST,
                        NRD.REVENUE_ACCOUNT_NAME,
                        NPD.PROD_GEN5,
                        NRD.ACCOUNT_ROLLUP1,
                        WATER.QUOTE_NUM,
                        WATER.DOC_DATE,
                        MNT.RQ_1_BACKLOG,
                        L_ACTUAL_PERIOD,
                        L_HRS_PERIOD,
                        L_START_PERIOD
                    FROM
                        NUAN_RPRO_AMOR_HRS_MATRIX_GT   HRS,
                        NUAN_RPRO_AMOR_CUR_MATRIX_GT   MNT,
                        NUAN_REVENUETYPE_DATA          NRD,
                        NUAN_PRODUCT_DATA              NPD,
                        NUAN_REVPRO_PROJECT_V          HIST,
                        (
                            SELECT DISTINCT
                                BUSINESS_UNIT,
                                SALES_REP_NAME,
                                F_CUR,
                                CURR,
                                CSTMR_NM,
                                CUST_NUM,
                                ATR2,
                                ATR3,
                                ATR13,
                                ATR12,
                                RC_ID,
                                DOC_LINE_ID   SO_LINE_ID,
                                DATE1,
                                DATE2,
                                DURATION      TERM,
                                ATR8,
                                ATR16,
                                ATR1,
                                QUOTE_NUM,
                                DOC_DATE,
                                ATR60,
                                DATE3         L_ACTUAL_PERIOD,
                                DATE4         L_HRS_PERIOD,
                                TRUNC(
                                    DATE5,
                                    'MM'
                                ) L_START_PERIOD
                            FROM
                                NUAN_RPRO_OPENRC_RUNTME_D_T
                            WHERE
                                REQUEST_ID = NVL(
                                    FND_GLOBAL.CONC_PRIORITY_REQUEST,
                                    FND_GLOBAL.CONC_REQUEST_ID
                                )
                        ) WATER,
                        LOB_ITD_HRS                    LOB
                    WHERE
                        1 = 1
                        AND SUBSTR(
                            MNT.REV_SEGMENTS,
                            15,
                            3
                        ) = NPD.PROD_FAMILY
                        AND SUBSTR(
                            MNT.REV_SEGMENTS,
                            8,
                            6
                        ) = NRD.REVENUE_ACCOUNT
                        AND MNT.PROJECT_ID = HIST.PROJECT_ID (+)
                        AND MNT.PROJECT_ID = HRS.PROJECT_ID (+)
                        AND MNT.TOP_TASK_NUMBER = HRS.TOP_TASK_NUMBER (+)
                        AND MNT.RC_ID = WATER.RC_ID
                        AND MNT.SO_LINE_ID = WATER.SO_LINE_ID
                        AND MNT.ATR25 = LOB.LOB_ATR25 (+)
                        AND MNT.PROJECT_ID = LOB.LOB_PROJECT_ID (+)
                        AND MNT.REQUEST_ID = HRS.REQUEST_ID (+)
                        AND MNT.REQUEST_ID = NVL(
                            FND_GLOBAL.CONC_PRIORITY_REQUEST,
                            FND_GLOBAL.CONC_REQUEST_ID
                        )
                )
            GROUP BY
                SOURCE,
                ATR25,
                ATR60,
                L_START_PERIOD,
                L_ACTUAL_PERIOD,
                RC_ID,
                SALES_ORDER_NUMBER,
                PROJECT_NUMBER,
                MS_SOURCE_ID,
                MS_SOURCE_NAME,
                PROJECT_DESCRIPTION,
                PROJECT_NAME,
                PROJECT_MANAGER,
                TECHNICAL_ACCOUNT_MANAGER,
                OPERATING_UNIT,
                PROJECT_OWNING_ORG,
                PROJECT_TYPE,
                PROGRAM,
                SALESREP_NAME,
                FUNCTIONAL_CURRENCY,
                TRANSACTIONAL_CURRENCY,
                SOLD_TO_CUSTOMER_NAME,
                SOLD_TO_CUSTOMER_NUMBER,
                BILL_TO_CUSTOMER_NAME,
                BILL_TO_CUSTOMER_NUMBER,
                SHIP_TO_CUSTOMER_NAME,
                SHIP_TO_CUSTOMER_NUMBER,
                PROJECT_STATUS,
                REV_ELEMENT,
                POB_TEMPLATE,
                DECODE(
                    TOP_TASK_NUMBER,
                    'USD',
                    NULL,
                    TOP_TASK_NUMBER
                ),
                TASK_DESCRIPTION,
                COMPANY,
                DEPARTMENT,
                ACCOUNT,
                PRODUCT_FAMILY,
                REGION,
                DEFERRED_ACCOUNT,
                DIVISION,
                PRACTICE,
                PS_OVERLAY,
                PLATFORM,
                REVENUE_METHOD_605,
                REVENUE_METHOD_606,
                PRODUCT_TYPE,
                ESTIMATED_GO_LIVE_DATE,
                ACTUAL_GO_LIVE_DATE,
                TERM,
                PROD_GEN2,
                REV_START_DATE,
                REV_END_DATE,
                LATEST_BASELINE_PM_FCST_DATE,
                LATEST_BASELINE_PM_FCST_HRS,
                ATR1,
                REVENUE_ACCOUNT_NAME,
                PROD_GEN5,
                ACCOUNT_ROLLUP1,
                QUOTE_NUM,
                DOC_DATE,
                ORIGINAL_BUDGET_HRS,
                --QE_BOOKING_AMT,
                QE_BOOKING_HRS,
                --NXT_QE_BOOKING_AMT,
                NXT_QE_BOOKING_HRS
        );
/