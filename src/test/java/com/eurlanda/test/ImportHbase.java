package com.eurlanda.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

/**
 * Created by YHC on 3/22/2017.
 * 为东航项目准备，导入Hbase数据库的代码
 */
public class ImportHbase {

    private static Configuration config;

    private static String TABLE_NAME = "TEST_DONGHANG_NEW";
    private static String CF_1_NAME = "USED_CF";
    private static String CF_2_NAME = "UNUSE_CF";


    private static String COLUMN_NAMES = "Flight_ID,Time,Frame-Sf,A_S_NWS_FAULT,A_S_SEL_PB,A_SKID_FLT_ONY,A_SKID_REF_SPD,A_THR_ENG1_BA,A_THR_ENG2_BA,AAPP,AC_ESS_BUS,AC_ESS_FEED_PB,AC_ESS_SHED_ON,AC_ESS_TR_CNTR,AC_IDEN,AC_TAIL1,AC_TAIL123,AC_TAIL2,AC_TAIL3,AC_TAIL4,AC_TAIL456,AC_TAIL4567,AC_TAIL7,AC_TYPE,AC_TYPE_BA,AC_TYPE_REC,AC1_AC_ESS_CNT,AC1_BUS_ON,AC2_AC_ESS_CNT,AC2_BUS,AC2_BUS_ON,ACARSSW270,ACC1_POS,ACC2_POS,ACMS_FP,ACSC1_FAULT,ACSC2_FAULT,ACTYPE,ADC_USED_CA,ADC_USED_FO,ADC1_FAULT,ADC1SW270,ADC2_FAULT,ADC2SW270,ADC3_FAULT,ADC3SW270,ADF1_BRG_SW_CA,ADF1_BRG_SW_FO,ADF1_FRQ,ADF1_TUNE_MODE,ADF2_BRG_SW_CA,ADF2_BRG_SW_FO,ADF2_FRQ,ADF2_TUNE_MODE,ADR_1_2_3_FLT,ADR_123_IN_FLT,ADS_B_FAULT,ADS_B_OFF,AFD,AFT,AFT_CGO_DET_F,AFT_CGO_DT_OH,AFT_CGO_DUCT_T,AFT_CGO_FAN_FT,AFT_CGO_HT_BUL,AFT_CGO_HT_CTF,AFT_CGO_HT_CTL,AFT_CGO_HT_DCL,AFT_CGO_HT_FF,AFT_CGO_HT_HA,AFT_CGO_HT_IC,AFT_CGO_HT_INS,AFT_CGO_ISO_PB,AFT_CGO_ISO_V,AFT_CGO_PRV_FC,AFT_CGO_TAV_PO,AFT_CGO_TEMP,AFT_CGO_TRIM_F,AIE1,AIE1_RW,AIE2,AIE2_RW,AIL_B_AVAIL_LH,AIL_B_AVAIL_RH,AIL_DROOP,AIL_G_AVAIL_LH,AIL_G_AVAIL_RH,AIL_LH,AIL_LH_B_FAULT,AIL_LH_G_FAULT,AIL_RH,AIL_RH_B_FAULT,AIL_RH_G_FAULT,AILL,AILR,AIR_DURN,AIRLINE_ID,AIRPORT_GA,AIRSTAIR_DOOR,AIW,AIW_RW,ALPHA_FLOOR,ALPHA_FLOOR_V,ALT_BARO_ADC1,ALT_BARO_ADC2,ALT_BARO_ADC3,ALT_BARO_CA,ALT_BARO_DISCR,ALT_BARO_FO,ALT_BRK_L_RELE,ALT_BRK_R_RELE,ALT_BRK_WN_INS,ALT_FEEDBACK1,ALT_FEEDBACK2,ALT_FL_PATH_CA,ALT_FL_PATH_FO,ALT_GPS,ALT_QNH,ALT_QNH_LD,ALT_QNH_TO,ALT_RATE,ALT_REF,ALT_SEL,ALT_STD,ALT_STD_CA,ALT_STD_DISCR,ALT_STD_FO,ALT_STD1,ALT_STD2,ALT_STD3,ALT_STDC,ALT2,ALTBARFN,ALTBARFO,ANCHOR_ALT_CA,ANCHOR_ALT_FO,ANCHOR_LAT_CA,ANCHOR_LAT_FO,ANCHOR_LONG_CA,ANCHOR_LONG_FO,ANTI_ICE_E1_PB,ANTI_ICE_E2_PB,AOA_IRS1,AOA_IRS2,AOA_IRS3,AOA_VOTED,AOA1_FAIL,AOA1_PROBE_FLT,AOA2_FAIL,AOA2_PROBE_FLT,AOA3_FAIL,AOA3_PROBE_FLT,AOAL,AOAMAX,AOAPROT,AOAR,AOCCOMMFAULT,AP_BA,AP_DISC_ALT,AP_EGD1,AP_EGD2,AP_ENG1_BA,AP_ENG2_BA,AP_ENG3_BA,AP_FD_TCAS_ARM,AP_FD_TCAS_EGD,AP_INST_DISC,AP_LANDTRACK,AP_LAT_HEAD,AP_LAT_LOCC,AP_LAT_LOCT,AP_LAT_NAV,AP_LAT_RFD,AP_LAT_RGA,AP_LAT_RW,AP_LAT_SALIGN,AP_LAT_SHEAD,AP_LAT_SPATH,AP_LAT_SRO,AP_LAT_SRW,AP_LAT_STRA,AP_LAT_SVOR,AP_LAT_TRACK,AP_LONG_ALT,AP_LONG_CAPT,AP_LONG_CLI,AP_LONG_DES,AP_LONG_EXP,AP_LONG_FDES,AP_LONG_FLARE,AP_LONG_FPA,AP_LONG_GS,AP_LONG_IMM,AP_LONG_OPEN,AP_LONG_PGA,AP_LONG_PTO,AP_LONG_TRK,AP_LONG_VS,AP_OFF_VOLUN,AP_OFF_VOLUNT,AP_OFF_WARN,AP_THR_BA,AP1_INOPERATE,AP1_INST_DISC,AP2_INOPERATE,AP2_INST_DISC,APP_NAV_DB,APPR_DH_CA,APPR_DH_FO,APPR_MODE,APPR_TYPE,APU_AUTO_S_D,APU_AVAIL,APU_BLD_FLOW,APU_BLD_PB,APU_BLD_PRESS,APU_BLD_V_FC,APU_BLEED,APU_CT5ATP,APU_DOOR_FAIL,APU_DP,APU_ECB_FAIL,APU_EGT,APU_EGT_LIM,APU_EMER_S_D,APU_ENG_ST_MOD,APU_EXT_USED,APU_FAULT_C2,APU_FIRE_PB,APU_FIRE_V_FC,APU_FIRE_V_FO,APU_FLAP_CLS,APU_FLAP_OPEN,APU_FUEL_LOPR,APU_GEN_CNTOR,APU_GEN_FLT,APU_GEN_FRQ,APU_GEN_LOAD,APU_GEN_PB,APU_GEN_VOLT,APU_IGV_COM,APU_IGV_POS,APU_LCIT,APU_LEAK_MEM,APU_LO_OIL_LV,APU_MAS_PB,APU_MAS_SW_PB,APU_N,APU_OIL_SW_FT,APU_OIL_TEMP,APU_P2_PRESS,APU_PACK_USED,APU_SCV_COM,APU_SCV_POS,APU_SD_DSW1,APU_SD_DSW10,APU_SD_DSW11,APU_SD_DSW12,APU_SD_DSW13,APU_SD_DSW14,APU_SD_DSW15,APU_SD_DSW16,APU_SD_DSW17,APU_SD_DSW18,APU_SD_DSW19,APU_SD_DSW2,APU_SD_DSW3,APU_SD_DSW4,APU_SD_DSW5,APU_SD_DSW6,APU_SD_DSW7,APU_SD_DSW8,APU_SD_DSW9,APU_START_PB,APU_START_PROG,APU_START_T,APU_USED,APUBLDVLVCLS,APUBLDVLVOP,APUFLAPACTCLS,APUFLAPACTOP,APUSW351,APUSW352,ARP_HEIGH_DEST,ARP_HEIGH_ORIG,ATC_MSG_AUDIO,ATC_MSG_LT,ATC1_FAULT,ATC1_STANDBY,ATC2_FAULT,ATC2_STANDBY,ATCAPPLYFLT,ATCCOMMFAULT,ATHR_ACT,ATHR_EGD,ATHR_INOPER,ATHR_PIN_PROG,ATHR_VOL_DISC,ATS_ACTIVE,ATS_ALPHA_MODE,ATS_EGD,ATS_IN_APPR,ATS_MODE,ATS_RET_MOD,ATS_SPD_MACH,ATS_SPDM_MODE,ATS_THRUSTN1,ATS1_INST_DISC,ATS2_INST_DISC,ATSU_FAULT,AUDIO_CODE,AUTBRFF2,AUTBRFT2,AUTBRKFF,AUTBRKFT,AUTO_BRK_STS,AUTO_LAND,AUTO_LAND_WN,AUTO_SPD_CTL,AUTO_V2,AVI_AF_DOOR_C,AVI_BLW_FAULT,AVI_BW_OVRD_PB,AVI_EX_OVRD_PB,AVI_EXT_FAULT,AVI_EXT_V_FC,AVI_EXT_V_FO,AVI_EXT_V_P_O,AVI_FW_DOOR_C,AVI_INT_V_FC,AVI_INT_V_FO,AVI_LH_DOOR_C,AVI_RH_DOOR_C,AVI_VENT_FAULT,AVIONI_SMK_DET,B_ELE_PMP_OVHT,B_ELEC_PUMP_LP,B_ELEC_PUMP_ON,B_ELEC_PUMP_PB,B_RSVR_LO_A_PR,B_RSVR_LO_LVL,B_RSVR_OV_HEAT,B3D_DAY,B3D_HOUR,B3D_MIN,B3D_MONTH,BACK_ALTI_GPS,BACK_SPD_VALUE,BACKUP_SPD_ACT,BACKUP_SPD_VAL,BAD_SF,BADSF_NR,BARO_SEL_CA,BARO_SEL_FO,BAROMB,BAT1_CNTOR,BAT1_CURRENT,BAT1_FAIL,BAT1_FAULT,BAT1_PB,BAT1_VOLTAGE,BAT2_CNTOR,BAT2_CURRENT,BAT2_FAIL,BAT2_FAULT,BAT2_PB,BAT2_VOLTAGE,BEA_MK,BLD_25_FAIL1,BLD_25_FAIL2,BLD_25_POS1,BLD_25_POS2,BLD_ABNORM_PR1,BLD_ABNORM_PR2,BLD_C2_FAIL_1,BLD_C2_FAIL_2,BLD_PS3_PO_1,BLD_PS3_PO_2,BLD_VLV1,BLD_VLV2,BLEED_E1_FAULT,BLEED_E2_FAULT,BOUNCE,BP1,BP2,BRAKE_FAN_PB1,BRAKE_FAN_PB2,BRAKE_MODE,BRG_4_PRS_E1,BRG_4_PRS_E2,BRG_TO_GO_CA,BRG_TO_GO_FO,BRK_FAN_INSTAL,BRK_PED_L_1,BRK_PED_L_2,BRK_PED_L1,BRK_PED_L2,BRK_PED_R_1,BRK_PED_R_2,BRK_PED_R1,BRK_PED_R2,BRK_PRESS1,BRK_PRESS2,BRK_PRESS3,BRK_PRESS4,BRK_RELEASE,BRK_SEL_V_FT1,BRK_SEL_V_FT2,BRK_SEL_V_POS1,BRK_SEL_V_POS2,BRK_TEMP1,BRK_TEMP2,BRK_TEMP3,BRK_TEMP4,BRK_Y_PRS_LH,BRK_Y_PRS_RH,BSCU1_EGD,BSCU1_MON_FLT,BSCU1_VALID,BSCU2_EGD,BSCU2_MON_FLT,BSCU2_VALID,BULK_DOOR_CLS,BUS_MSG_FWC1,BUS_TIE_PB,CAB_ALT_CRUISE,CAB_ALT_MAX,CAB_DIFF_PRS1,CAB_DIFF_PRS2,CAB_DOOR_LH_A,CAB_DOOR_LH_F,CAB_DOOR_RH_A,CAB_DOOR_RH_F,CAB_FAN_LH_FLT,CAB_FAN_PB,CAB_FAN_RH_FLT,CAB_OXY_REG1_F,CAB_OXY_REG2_F,CAB_OXY_REGLOP,CAB_PRS_MOD_PB,CAB_PRS_RES_PR,CAB_PRS_WAR,CABIN_ALT_SYS1,CABIN_ALT_SYS2,CABIN_READY,CALL_BY_ADV,CAOA_ADC1,CAOA_ADC2,CAS_ADC1,CAS_ADC2,CAS_ADC3,CB_TRIP_AVIO_L,CB_TRIP_AVIO_R,CB_TRIP_J_M,CB_TRIP_N_R,CB_TRIP_OVHEAD,CB_TRIP_S_V,CB_TRIP_W_Z,CDSS_SEL_SD,CENT_CURR,CENT_TO_BA,CFDIU_BKUP_FLT,CFDIU_FAIL,CFDIU_NORM_FLT,CG,CG_DR_A_HDL_L1,CG_DR_A_HDL_L2,CG_DR_A_SAF_L1,CG_DR_A_SAF_L2,CG_DR_A_SFT_L1,CG_DR_A_SFT_L2,CG_DR_F_HDL_L1,CG_DR_F_HDL_L2,CG_DR_F_SAF_L1,CG_DR_F_SAF_L2,CG_DR_F_SFT_L1,CG_DR_F_SFT_L2,CG_FAC,CG_FMC,CHECK_ADC,CHECK_IRS,CHK_HDG_WARN,CIDS1_CAUTION,CIDS2_CAUTION,CITY_FROM,CITY_FROM_R,CITY_TO,CITY_TO_R,CK_ALT_SEL,CK_ALT_STD,CK_C1_R1_COL,CK_C1_R1_TXT,CK_C1_R3_COL,CK_C1_R3_TXT,CK_C2_R1_COL,CK_C2_R1_TXT,CK_C2_R2_COL,CK_C2_R2_TXT,CK_C2_R3_COL,CK_C2_R3_TXT,CK_C3_R1_COL,CK_C3_R1_TXT,CK_C3_R2_COL,CK_C3_R2_TXT,CK_C4_R1_COL,CK_C4_R1_TXT,CK_C4_R2_COL,CK_C4_R2_TXT,CK_C4_R3_COL,CK_C4_R3_TXT,CK_C5_R1_COL,CK_C5_R1_TXT,CK_C5_R2_COL,CK_C5_R2_TXT,CK_C5_R3_COL,CK_C5_R3_TXT,CK_CONF_POS,CK_CONF_POS_PC,CK_CONF_SCL_MX,CK_EGT_SCL_MAX,CK_EGT1,CK_EGT2,CK_EPR_SCL_MAX,CK_EPR1,CK_EPR2,CK_FLAP_SPD,CK_FQTY,CK_GPWS_MODE,CK_GW,CK_HEAD,CK_HEAD_MAG,CK_HEAD_SEL,CK_HEAD_SELON,CK_IAS,CK_INS_ENG_MOD,CK_ITT_SCL_MAX,CK_LDG_NR,CK_LDG_SELDW,CK_LDG_WOW,CK_MACH,CK_MAS_CAU,CK_MAS_WAR,CK_N1_SCL_MAX,CK_N11,CK_N12,CK_N21,CK_N22,CK_PITCH_CPT,CK_PITCH_FO,CK_RALT,CK_REV_DEP1,CK_REV_DEP2,CK_ROLL_CPT,CK_ROLL_FO,CK_RUDD,CK_SPD_BRK,CK_TCAS_RA,CK_TCAS_TA,CK_TLA_PCT1,CK_TLA_PCT2,CK_TLA_SCL_MAX,CK_TLA_SCL_MIN,CK_TORQ_SCL_MX,CK_V1,CK_V2,CK_VAPP,CK_VHF1_EMIT,CK_VLS,CK_VMAX,CK_VR,CONF,CONF_LD,CONF_TO,CONSALTD,CPC_SYS_1_2_FT,CPC1_ADC_USED,CPC1_ALT_WARN,CPC1_CTL,CPC1_DES_QUICK,CPC1_ELEV_MAN,CPC1_FAIL,CPC1_FLT_MODE,CPC1_FMS_ABLE,CPC1_FMS_NU,CPC1_FMS_SEL,CPC1_LFE_USED,CPC1_LO_PR_DIF,CPC1_QNH_NU,CPC2_ADC_USED,CPC2_ALT_WARN,CPC2_CTL,CPC2_DES_QUICK,CPC2_ELEV_MAN,CPC2_FAIL,CPC2_FLT_MODE,CPC2_FMS_ABLE,CPC2_FMS_NU,CPC2_FMS_SEL,CPC2_LFE_USED,CPC2_LO_PR_DIF,CPC2_QNH_NU,CREW_OXY_LP,CREW_OXY_PRS,CRUISE_FL,CSAS_ISO_V_V1,CSAS_ISO_V_V3,CSAS_TEMP,CTK_MAN_SEL_PB,CTK_PUMP1_AUTO,CTK_PUMP2_AUTO,CUT,CYCLECNT,DA1,DAR_FAIL,DAR_TAPE_LOW,DAT_DAY,DAT_MONTH,DATE,DATE_R,DATE_TO,DATE_YEAR,DAY_CURR,DAY_LD,DAY_TO,DAY_TO_BA,DB_CYC_CA,DB_CYC_FO,DB_DAT_CA,DB_DAT_FO,DB_MON_CA,DB_MON_FO,DC_ESS_ON,DC1_BUS,DC1_DC_ESS_CNT,DC2_BUS,DC2_DC_ESS_CNT,DCBAT_DCESS_CN,DCBAT_DCESS_SH,DECEL_LOW_CA,DECEL_LOW_FO,DECEL_MAX_CA,DECEL_MAX_FO,DECEL_MID_CA,DECEL_MID_FO,DESTINATION,DESTINATION_BA,DEV_MAG,DFC,DFDR_GND_CTL,DFDR_STS_FAIL,DH_NOT_SEL,DH_SEL_CA,DH_SEL_FO,DISC_AP_MSG_CA,DISC_AP_MSG_FO,DIST,DIST_BY_GS,DIST_LANDING,DIST_LDG,DIST_TO,DMC_IDENT,DMC1_INVALID,DMC13_FLT_FWC,DMC2_INVALID,DMC23_FLT_FWC,DMC3_INVALID,DMC3_TRANS_CA,DMC3_TRANS_FO,DMC3_XFR_CA,DMC3_XFR_FO,DMC301AL,DME_DIST_GP_CA,DME_DIST_GP_FO,DME_DIST_RES1,DME_DIST_RES2,DME_FRQ1,DME_FRQ2,DME1_DST,DME2_DST,DMU_DB_SW,DMU_FAIL,DMU_SYS_SW,DN_STM_ISO_V_C,DRIFT_200FT,DRIFT_50FT,DRIFT_DMC,DRIFT_LAND,DTO_MAN_DISP,DTO_MATRIX,DTO_MODE_SEL,DUAL_INPUT,DUCT_TEMP_A_C,DUCT_TEMP_C_P,DUCT_TEMP_F_C,DUR_CRUISE,DURN_200_LD,E1_A_I_V_FAULT,E1_AI_V_FAULT,E2_A_I_V_FAULT,E2_AI_V_FAULT,ECAM_CLR_PB,ECAM_DU1,ECAM_DU2,ECAM_EMER_PB,ECAM_ND_XFR_CA,ECAM_ND_XFR_FO,ECAM_RECALL_PB,ECAM_SEL_APU,ECAM_SEL_BLED,ECAM_SEL_CFA,ECAM_SEL_COND,ECAM_SEL_CRUIS,ECAM_SEL_DOOR,ECAM_SEL_ELEC,ECAM_SEL_ENG,ECAM_SEL_FCTL,ECAM_SEL_FUEL,ECAM_SEL_HYD,ECAM_SEL_PRES,ECAM_SEL_STAT,ECAM_SEL_WHEL,ECAM_STATUS_PB,EEC1_B_CTL,EEC1_C2_FAULT,EEC1_DSW_145,EEC1_DSW_146,EEC1_DSW_155,EEC1_DSW_156,EEC1_DSW_270,EEC1_DSW_271,EEC1_DSW_276,EEC1_DSW_352,EEC1_NODATA,EEC2_B_CTL,EEC2_C2_FAULT,EEC2_DSW_145,EEC2_DSW_146,EEC2_DSW_155,EEC2_DSW_156,EEC2_DSW_270,EEC2_DSW_271,EEC2_DSW_276,EEC2_DSW_352,EEC2_NODATA,EGP1,EGP2,EGPWS_INSTA_CA,EGPWS_INSTA_FO,EGPWS_PEAK,EGT_DISCR_1,EGT_DISCR_2,EGT_MAX,EGT_MAX1,EGT_MAX1_START,EGT_MAX2,EGT_MAX2_START,EGT_OVERLIM_1,EGT_OVERLIM_2,EGT1,EGT1_DMC,EGT1C,EGT2,EGT2_DMC,EGT2C,EIS2_DU_CHK_MG,EIS2_INSTALL,ELAC_ACTIVE,ELAC1_FAULT,ELAC1_PB,ELAC1_PITCH,ELAC1_ROLL,ELAC2_FAULT,ELAC2_PB,ELAC2_PITCH,ELAC2_ROLL,ELEV_L_R_FLT,ELEV_LH,ELEV_LH_B_AVAI,ELEV_LH_B_FLT,ELEV_LH_G_AVAI,ELEV_LH_G_FLT,ELEV_RH,ELEV_RH_B_AVAI,ELEV_RH_B_FLT,ELEV_RH_Y_AVAI,ELEV_RH_Y_FLT,ELEV1,ELEV2,EMER_DOOR_LA0,EMER_DOOR_LA1,EMER_DOOR_LF0,EMER_DOOR_LF1,EMER_DOOR_RA0,EMER_DOOR_RA1,EMER_DOOR_RF0,EMER_DOOR_RF1,EMER_GEN_FRQ,EMER_GEN_LINE,EMER_GEN_VOLT,ENG_DUAL_FLT,ENG_MAN,ENG_SN1,ENG_SN1_LSB,ENG_SN1_MSB,ENG_SN2,ENG_SN2_LSB,ENG_SN2_MSB,ENG1_BLD_LOW_T,ENG1_BLEED_PB,ENG1_CRANK_SEL,ENG1_FIRE_PB,ENG1_IGN_SEL,ENG1_MAN_SEL,ENG1_MAS_LVR,ENG1_MOD_SEL,ENG1_NORM_SEL,ENG1_POS_ER,ENG1_PT25,ENG1_SEVER_ICE,ENG2_BLD_LOW_T,ENG2_BLEED_PB,ENG2_CRANK_SEL,ENG2_FIRE_PB,ENG2_IGN_SEL,ENG2_MAN_SEL,ENG2_MAS_LVR,ENG2_MOD_SEL,ENG2_NORM_SEL,ENG2_POS_ER,ENG2_PT25,ENG2_SEVER_ICE,ENGINE_OUT,ENGTYPE_R,ENGVER,EPE_CA,EPE_FO,EPR_BA,EPR_DISCR_1,EPR_DISCR_2,EPR_LIMIT_1,EPR_LIMIT_2,EPR_MAX,EPR_TARGET_1,EPR_TARGET_2,EPR1,EPR1_DMC,EPR1_MAX,EPR1C,EPR2,EPR2_DMC,EPR2_MAX,EPR2C,EPRC1,EPRC2,EPRTG1,ESNL1,ESNL2,ESNL3,ESNL4,ESNL5,ESNL6,ESNR1,ESNR2,ESNR3,ESNR4,ESNR5,ESNR6,ESS_TR_CNTOR,ESS_TRU_CURR,ESS_TRU_FAULT,ESS_TRU_VOLT,EVENT,EVMU1_C1_FLT,EVMU1_C2_FLT,EVMU2_C1_FLT,EVMU2_C2_FLT,EXT_PWR_AVAIL,EXT_PWR_CNTOR,EXT_PWR_FRQ,EXT_PWR_PB,EXT_PWR_VOLT,EXT_TMP_ENG1,EXT_TMP_ENG2,F_APP_MSG_CA,F_APP_MSG_FO,F_APP_RAW_CA,F_APP_RAW_FO,FAC_ADIRS_SWIT,FAC_USED_CA,FAC_USED_FO,FAC1_FAIL,FAC1_HEALTHY,FAC2_FAIL,FAC2_HEALTHY,FADEC1_FAULT,FADEC2_FAULT,FAV1_FC,FAV1_FO,FAV2_FC,FAV2_FO,FBURN,FBURN_AVG,FBURN_TI,FBURN1,FBURN2,FCDC1_FAULT,FCDC2_FAULT,FCTL_CLS2_F,FCU_ALT_CHANGE,FCU_ALT_PULL,FCU_ALT_PUSH,FCU_APPR_PB,FCU_DAT_LH,FCU_DAT_RH,FCU_DATA_DW,FCU_DATA_UP,FCU_EXPED_PB,FCU_FCU1_FAIL,FCU_FCU1_HEAL,FCU_FCU1_SEL,FCU_FCU2_FAIL,FCU_FCU2_HEAL,FCU_FCU2_SEL,FCU_FDOFF_LH,FCU_FDOFF_RH,FCU_FMGC1_FAIL,FCU_FMGC1_SEL,FCU_FMGC2_FAIL,FCU_FMGC2_SEL,FCU_FPA_SEL,FCU_LAT_PULL,FCU_LAT_PUSH,FCU_LAT_SET,FCU_LOC_PRES,FCU_MACH_SEL,FCU_METRIC_SEL,FCU_POW_PULSE,FCU_SEL_SPD,FCU_SPD_CHANGE,FCU_SPD_PB,FCU_SPD_PULL,FCU_SPD_PUSH,FCU_V_S_SEL,FCU_VRT_CHANGE,FCU_VRT_PULL,FCU_VRT_PUSH,FCU1A_FAIL,FCU1B_FAIL,FCU2721,FCU2A_FAIL,FCU2B_FAIL,FD_1,FD_2,FD_BAR_REMOVE,FD_LIGHT_ON_CA,FD_LIGHT_ON_FO,FDIU_FAIL,FDPTCH_CA,FDPTCH_FO,FDROLL_CA,FDROLL_FO,FDV1_FAIL,FDV1_SOLE_FAIL,FDV2_FAIL,FDV2_SOLE_FAIL,FDYAW_CA,FDYAW_FO,FF_DISCR_1,FF_DISCR_2,FF1,FF1_DMC,FF1_MAX,FF1C,FF2,FF2_DMC,FF2_MAX,FF2C,FFV_ENG1_FC,FFV_ENG2_FC,FG1_FAIL,FG2_FAIL,FGC_FD_CA,FGC_FD_FO,FGC_FMA_CA,FGC_FMA_FO,FGC041,FGC273,FGC274,FILE_NO,FIRE_APU,FIRE_BOT_LOP_A,FIRE_BOT1_LOP1,FIRE_BOT1_LOP2,FIRE_BOT2_LOP1,FIRE_BOT2_LOP2,FIRE_CH_A_APU,FIRE_CH_A_E1,FIRE_CH_A_E2,FIRE_CH_A_INO1,FIRE_CH_A_INO2,FIRE_CH_B_APU,FIRE_CH_B_E1,FIRE_CH_B_E2,FIRE_CH_B_INO1,FIRE_CH_B_INO2,FIRE_CHA_INO_A,FIRE_CHB_INO_A,FIRE1,FIRE2,FL_CRUISE,FLAP,FLAP_AUTO_CMD,FLAP_FAULT,FLAP_LEVEL1,FLAP_LEVEL2,FLAP_LEVER_CFA,FLAP_LH_CONNEC,FLAP_LH_SENSOR,FLAP_LVR_N_Z,FLAP_POS_FWC,FLAP_POS_LH,FLAP_POS_RH,FLAP_RH_CONNEC,FLAP_RH_SENSOR,FLAP_SPD,FLAP_SYS1_FLT,FLAP_SYS2_FLT,FLAPC,FLAPRW,FLARE_LAW_ACT,FLEET_ID,FLEX_TEMP1,FLEX_TEMP2,FLEX_TO1,FLEX_TO2,FLHV,FLIGHT_CHANG,FLIGHT_IDENT,FLIGHT_INPUT,FLIGHT_NO1,FLIGHT_NO1_BA,FLIGHT_NO2,FLIGHT_NO2_BA,FLIGHT_PHAS_BA,FLIGHT_PHASE,FLIGHT_RECOGN,FLIGHT_TYPE,FLT_PHASE_D,FLT_PHASE_DMU,FLTNO1,FLTNO2,FLTNO3,FLTNO4,FLTNO5,FLTNO6,FLTNO7,FLTNO8,FLTNUM,FM_GPS_POS_DIS,FM1_FAIL,FM2_FAIL,FMA_ALT_ARM_D,FMA_ALT_ARM_F,FMA_ALT_MOD,FMA_ALT_POSSI,FMA_APPR_PHASE,FMA_ATHR,FMA_ATHR_BA,FMA_ATS,FMA_ATS_DISP,FMA_CLB,FMA_CLB_ARM,FMA_CPTU_MOD,FMA_DASH_DIS,FMA_DCT,FMA_DES_ARM,FMA_EXP_MOD,FMA_FD_MOD,FMA_FINAL_DES,FMA_FLR_MOD,FMA_FPA_MOD,FMA_GA,FMA_GS_ARM,FMA_GS_BEF_LOC,FMA_GS_MOD,FMA_HDG,FMA_HDG_PRESET,FMA_IMT,FMA_LAND_ARM,FMA_LAT,FMA_LAT_BA,FMA_LAT_CFA,FMA_LAT_RESET,FMA_LD_TRK,FMA_LOC_ARM,FMA_LOC_BACK,FMA_LOK_CPT,FMA_LOK_TRK,FMA_LONG,FMA_LONG_BA,FMA_LONG_CFA,FMA_LONG_FLASH,FMA_LONG_MODE,FMA_LONG_RESET,FMA_MSG_CK_AP,FMA_MSG_CONF,FMA_MSG_DECEL,FMA_MSG_DEG,FMA_MSG_DISC,FMA_MSG_DRAG,FMA_MSG_G_D,FMA_MSG_GEAR,FMA_MSG_HOLD,FMA_MSG_M_A,FMA_MSG_NAVA,FMA_MSG_UPG,FMA_NAV_ARM,FMA_NAV_MOD,FMA_OPEN,FMA_PITCH_GA,FMA_PITCH_TO,FMA_QFU_CMD,FMA_RUN_MOD,FMA_SPD_PRESET,FMA_TRK_MOD,FMA_TRK_MOD2,FMA_VRT_SPD,FMC_USED_CA,FMC_USED_FO,FORE_CGO_DET_F,FPA,FPA_REC,FPA_SEL,FPAC,FQ_CENTER,FQ_LH_INNER,FQ_LH_OUTER,FQ_RH_INNER,FQ_RH_OUTER,FQI1_FAIL,FQI2_FAIL,FQTY_ENG_OFF,FQTY_ENG_ON,FQTY_LD,FQTY_TO,FQU_ACT1_FIT,FQU_ACT2_FIT,FQU_ATV_OPEN_F,FQU_ATV_SHUT_F,FQU_CH1_CL2_F,FQU_CH1_F,FQU_CH1_TMP_F,FQU_CH2_CL2_F,FQU_CH2_F,FQU_CH2_PRIO,FQU_CH2_TMP_F,FQU_CHA_IMBAL,FQU_CT_FIT,FQU_CT_NEMPTY,FQU_CTLH_FLT,FQU_CTRH_FLT,FQU_LH_LTMP_WN,FQU_LHBTV_F,FQU_LHFTV_OPEN,FQU_LHFTV_SHUT,FQU_LHRTV_OPEN,FQU_LHRTV_SHUT,FQU_LI_HT_ADV,FQU_LI_HT_WN,FQU_LI_LT_ADV,FQU_LI_LT_WN,FQU_LO_HT_ADV,FQU_LO_HT_WN,FQU_LO_LT_ADV,FQU_LO_LT_WN,FQU_LO_T_STS,FQU_REF_ABORT,FQU_REF_COMP,FQU_REF_POW,FQU_REF_PROG,FQU_REF_SEL,FQU_RH_LTMP_WN,FQU_RHBTV_F,FQU_RHFTV_OPEN,FQU_RHFTV_SHUT,FQU_RHRTV_OPEN,FQU_RHRTV_SHUT,FQU_RI_HT_ADV,FQU_RI_HT_WN,FQU_RI_LT_ADV,FQU_RI_LT_WN,FQU_RO_HT_ADV,FQU_RO_HT_WN,FQU_RO_LT_ADV,FQU_RO_LT_WN,FQU_RO_T_STS,FR_COUNT,FROM1,FROM2,FROM3,FROM4,FTIS_FAULT,FUEL_F_V_1_FC,FUEL_F_V_1_FO,FUEL_F_V_2_FC,FUEL_F_V_2_FO,FUEL_FIL_CLOG1,FUEL_FIL_CLOG2,FUEL_LEAK_ALET,FUEL_QUANTITY,FUEL_T_CTL_FT1,FUEL_T_CTL_FT2,FUEL_TF_FAIL1,FUEL_TF_FAIL2,FUEL_UNIT_M_E,FUEL_USED_AIR1,FUEL_USED_AIR2,FUEL_USED1,FUEL_USED2,FUEL_XFEED_PB,FUEL_XFEED_VFC,FUEL_XFEED_VFO,FWC_VALID,FWC_WARN_MSG1,FWC_WARN_MSG2,FWC01510,FWC1_FAIL,FWC1_MSG_FAIL,FWC1_VALID,FWC2_FAIL,FWC2_MSG_FAIL,FWC2_VALID,G_E1_PUMP_LP,G_E1_PUMP_PB,G_RSVR_LO_A_PR,G_RSVR_LO_LVL,G_RSVR_OV_HEAT,GALLEY_SHED_PB,GAMAX1000,GAMAX150,GAMAX500,GAMIN1000,GAMIN150,GAMIN500,GAZ_OXY_PAX_IN,GEAR_DNLCK_WN,GEAR_LDR_NOUP1,GEAR_LDR_NOUP2,GEAR_LEV_DN_BA,GEAR_LH_DWLK1,GEAR_LH_DWLK2,GEAR_LH_FE_L1,GEAR_LH_FE_L2,GEAR_LH_NOLKD1,GEAR_LH_NOLKD2,GEAR_LH_NOLKU1,GEAR_LH_NOLKU2,GEAR_LH_NUP_L1,GEAR_LH_NUP_L2,GEAR_LH_SANE1,GEAR_LH_SANE2,GEAR_LH_UPLK1,GEAR_LH_UPLK2,GEAR_NDR_NOUP1,GEAR_NDR_NOUP2,GEAR_NS_DWLK1,GEAR_NS_DWLK2,GEAR_NS_FE_L1,GEAR_NS_FE_L2,GEAR_NS_NOLKD1,GEAR_NS_NOLKD2,GEAR_NS_NOLKU1,GEAR_NS_NOLKU2,GEAR_NS_NUP_L1,GEAR_NS_NUP_L2,GEAR_NS_SANE1,GEAR_NS_SANE2,GEAR_NS_UPLK1,GEAR_NS_UPLK2,GEAR_RDR_NOUP1,GEAR_RDR_NOUP2,GEAR_RH_DWLK1,GEAR_RH_DWLK2,GEAR_RH_FE_L1,GEAR_RH_FE_L2,GEAR_RH_NOLKD1,GEAR_RH_NOLKD2,GEAR_RH_NOLKU1,GEAR_RH_NOLKU2,GEAR_RH_NUP_L1,GEAR_RH_NUP_L2,GEAR_RH_SANE1,GEAR_RH_SANE2,GEAR_RH_UPLK1,GEAR_RH_UPLK2,GEN1_FAULT,GEN1_FRQ,GEN1_LINE_CNTR,GEN1_LINE_PB,GEN1_LOAD,GEN1_OVERLOAD,GEN1_PB,GEN1_VOLT,GEN2_FAULT,GEN2_FRQ,GEN2_LINE_CNTR,GEN2_LOAD,GEN2_OVERLOAD,GEN2_PB,GEN2_VOLT,GLA_MLA_ACT,GLID_BA,GLIDE_DEV_MAX,GLIDE_DEV_MIN,GLIDE_DEV1,GLIDE_DEV1_D,GLIDE_DEV2,GLIDE_DEV2_D,GLIDE_DEVC,GLIDE_GAP,GLIDE_ILS,GND_FLT_BOOLEA,GND_SPOILER,GPIRS_HFOM_CA,GPIRS_HFOM_FO,GPS_GS_CA,GPS_GS_FO,GPS_HFOM_CA,GPS_HFOM_FO,GPS_HIL_CA,GPS_HIL_FO,GPS_LAT_CA,GPS_LAT_F_CA,GPS_LAT_F_FO,GPS_LAT_FO,GPS_LONG_CA,GPS_LONG_F_CA,GPS_LONG_F_FO,GPS_LONG_FO,GPS_LOST,GPS_LOST_CA,GPS_LOST_FO,GPS_PRIMARY,GPS_PRIMARY_CA,GPS_PRIMARY_FO,GPW0_BA,GPW1_BA,GPW2_BA,GPW3_BA,GPW4_BA,GPW5_BA,GPW6_BA,GPW7_BA,GPW8_BA,GPWS_ALT_EN,GPWS_AUDIO,GPWS_CONF3_PB,GPWS_CTYPE,GPWS_DH,GPWS_DONT_SINK,GPWS_DSW_270,GPWS_EGPWS_FLT,GPWS_ENV_PRO1,GPWS_ENV_SNAP1,GPWS_ETERR_CA,GPWS_ETERR_FO,GPWS_FLAP_LOW,GPWS_FLAP_PB,GPWS_GEAR_LOW,GPWS_GLIDE,GPWS_GS_CANCEL,GPWS_GS_INH,GPWS_INHIBIT1,GPWS_INHIBIT7,GPWS_LD_FLAP,GPWS_LD_GEAR,GPWS_MOD3_VIS1,GPWS_MOD4_VIS1,GPWS_MOD5_VIS1,GPWS_MOD6_LV,GPWS_MOD7_AL1,GPWS_MOD7_WN1,GPWS_MODE,GPWS_MVS,GPWS_PULL_UP,GPWS_PULLUP1,GPWS_PULLUP2A,GPWS_SINK_RATE,GPWS_SINK1,GPWS_ST_EGPWS,GPWS_TAD_CAU1,GPWS_TAD_WN1,GPWS_TCF_AL1,GPWS_TERR_AVAI,GPWS_TERR_FLT,GPWS_TERR_PB,GPWS_TERR_TAD,GPWS_TERRAIN,GPWS_TERRAIN1,GPWS_TO_MODE1,GPWS_TR_AH,GPWS_TR_AH_UP,GPWS_TR_LOW,GPWS_TR_UP,GPWS_VALID_CA,GPWS_VALID_FO,GPWS_VSL_ALERT,GPWS_WAR,GPWS_WAR_MOD,GPWS_WR1_PB,GPWS_WR2_PB,GPWS_WS_AI1,GPWS_WS_TO1,GPWS_WS_VALID1,GPWS_WSH_WAR1,GPWS_WSH_WAR2,GPWS270,GPWS271,GS,GS_CA,GS_EXIT,GS_FO,GS_LAT,GS_LONG,GS_TO,GSC,GW,GW_BA,GW_LD,GW_TO,GWC,HDG_VS_SEL,HEAD,HEAD_CA,HEAD_FO,HEAD_LIN,HEAD_MAG,HEAD_SEL,HEAD_SELON,HEAD_T,HEAD_TRUE,HEIG_1CONF_CHG,HEIG_DEV_1000,HEIG_DEV_500,HEIG_GEAR_DW,HEIG_LCONF_CHG,HEIGH_DEST,HEIGH_ORIG,HEIGHT,HF1,HF2,HI_ALT_PB,HIGHSPDT,HOUR_TO_BA,HP_FUEL_SOV1,HP_FUEL_SOV2,HPV_ENG1,HPV_ENG1_R,HPV_ENG2,HPV_ENG2_R,HPV1_FAULT,HPV1_FO,HPV1_SOLEN_EGD,HPV2_FAULT,HPV2_FO,HPV2_SOLEN_EGD,HT_CONF_CHG_01,HT_CONF_CHG_10,HT_CONF_CHG_12,HT_CONF_CHG_21,HT_CONF_CHG_23,HT_CONF_CHG_32,HT_CONF_CHG_34,HT_CONF_CHG_43,HT_CONF_CHG_45,HT_CONF_CHG_54,HT_CONF_CHG_56,HT_CONF_CHG_65,HT_CONF_CHG_67,HT_CONF_CHG_76,HT_CONF_CHG_78,HT_CONF_CHG_87,HYD_B_LO_PR,HYD_B_REV_QUNT,HYD_B_REV_TEMP,HYD_G_ENG_PUMP,HYD_G_FIR_V_FC,HYD_G_LO_PR,HYD_G_REV_QUNT,HYD_G_REV_TEMP,HYD_PRS_B,HYD_PRS_G,HYD_PRS_Y,HYD_Y_ENG_PUMP,HYD_Y_FIR_V_FC,HYD_Y_LO_PR,HYD_Y_REV_QUNT,HYD_Y_REV_TEMP,IAOA_CA,IAOA_FO,IAS,IAS_APP_1000,IAS_APP_50,IAS_APP_500,IAS_BEF_LD,IAS_CA,IAS_CL_1000,IAS_CL_500,IAS_EXT_CONF1,IAS_EXT_CONF2,IAS_EXT_CONF3,IAS_EXT_CONF4,IAS_EXT_CONF5,IAS_FO,IAS_LD,IAS_MAX,IAS_MAX_GEARDW,IAS_MAXCNF1LD,IAS_MAXCNF2LD,IAS_MAXCNF3LD,IAS_MAXCNF4LD,IAS_MAXCNF5LD,IAS_MAXCNF6LD,IAS_MAXCNF7LD,IAS_MAXCNF8LD,IAS_MAXCONF1TO,IAS_MAXCONF2TO,IAS_MAXCONF3TO,IAS_MAXCONF4TO,IAS_MAXCONF5TO,IAS_MIN_FINAPP,IAS_TO,IAS_TO_50,IAS1_MAX_MEM,IAS2_MAX_MEM,IAS3_MAX_MEM,IASC,ICE_DET_WARN1,ICE_DET_WARN2,ICE_DET1_FAULT,ICE_DET2_FAULT,ID_APPR_CA1,ID_APPR_CA2,ID_APPR_CA3,ID_APPR_CA4,ID_APPR_CA5,ID_APPR_CA6,ID_APPR_CA7,ID_APPR_CA8,ID_APPR_CA9,ID_APPR_FO123,ID_APPR_FO456,ID_APPR_FO789,ID_COPYRIGHT1,ID_COPYRIGHT10,ID_COPYRIGHT11,ID_COPYRIGHT12,ID_COPYRIGHT13,ID_COPYRIGHT2,ID_COPYRIGHT3,ID_COPYRIGHT4,ID_COPYRIGHT5,ID_COPYRIGHT6,ID_COPYRIGHT7,ID_COPYRIGHT8,ID_COPYRIGHT9,IDG1_DISC,IDG1_DISC_PB,IDG1_OIL_LO_PR,IDG1_OIL_OVHT,IDG1_OIL_TEMP,IDG2_DISC,IDG2_DISC_PB,IDG2_OIL_LO_PR,IDG2_OIL_OVHT,IDG2_OIL_TEMP,ILS_FRQ1,ILS_FRQ2,ILS_LIM,ILS_MSG_CA,ILS_MSG_FO,ILS_PB_CA,ILS_PB_FO,ILS_RAW_PFD_CA,ILS_RAW_PFD_FO,ILS_SEL_CRS1,ILS_SEL_CRS2,ILS_TUNE_MODE,ILS_VAL,IN_FLIGHT,INITIAL_FOB,INS_DISC_ATS,IR1_EXCESS_MOT,IR1_POS_DISAGR,IR1_POS_MISS,IR2_EXCESS_MOT,IR2_POS_DISAGR,IR2_POS_MISS,IR3_EXCESS_MOT,IR3_POS_DISAGR,IR3_POS_MISS,IRS_MAINT_CODE,IRS_USED_CA,IRS_USED_FO,IRS1_ADC1_FLT,IRS1_ADR_FAULT,IRS1_ALIGN,IRS1_ALIGN_FLT,IRS1_ALIGN_STA,IRS1_ATT,IRS1_ATT_FLT,IRS1_DC_FAIL,IRS1_DC_ON,IRS1_DC18_FAIL,IRS1_EXC_ERR,IRS1_EXTR_LAT,IRS1_FAULT,IRS1_IR_INI,IRS1_IRU_FAIL,IRS1_NAV,IRS1_SETHEAD,IRS2_ADC1_FLT,IRS2_ADR_FAULT,IRS2_ALIGN,IRS2_ALIGN_FLT,IRS2_ALIGN_STA,IRS2_ATT,IRS2_ATT_FLT,IRS2_DC_FAIL,IRS2_DC_ON,IRS2_DC18_FAIL,IRS2_EXC_ERR,IRS2_EXTR_LAT,IRS2_FAULT,IRS2_IR_INI,IRS2_IRU_FAIL,IRS2_NAV,IRS2_SETHEAD,IRS3_ADC1_FLT,IRS3_ADR_FAULT,IRS3_ALIGN,IRS3_ALIGN_FLT,IRS3_ALIGN_STA,IRS3_ATT,IRS3_ATT_FLT,IRS3_DC_FAIL,IRS3_DC_ON,IRS3_DC18_FAIL,IRS3_EXC_ERR,IRS3_EXTR_LAT,IRS3_FAULT,IRS3_IR_INI,IRS3_IRU_FAIL,IRS3_NAV,IRS3_SETHEAD,IVV,IVV_CA,IVV_FO,IVV_MAX_BL2000,IVV_MIN_CL35,IVV_MIN_CL400,IVV_SEL,IVVR,LAND_CAPA_DISP,LAND_CONF3,LAND_LT_LH_EXT,LAND_LT_RH_EXT,LAND2_CAPA,LAND2_INOP,LAND3_FOC,LAND3_FOIN,LAND3_FPC,LAND3_FPIN,LAT_AIR,LAT_ANG1,LAT_ANG2,LAT_DEV,LAT_DEV_CA,LAT_DEV_FO,LAT_POS_CA,LAT_POS_FO,LAT_RES_CA,LAT_RES_FO,LATG,LATG_IRS,LATP,LATPC,LAV_GAY_FAN_ON,LAV_SMK_R_OR_A,LDEV_DISP_CA,LDEV_DISP_FO,LDEV_VDEV_CA,LDEV_VDEV_FO,LDG_SELDW,LDG_SELDW1,LDG_SELDW2,LDG_SELUP,LDG_SELUP1,LDG_SELUP2,LDG_UNLOKDW,LDGL,LDGNOS,LDGR,LG_BOGIE_L_L1,LG_BOGIE_L_L2,LG_BOGIE_R_L1,LG_BOGIE_R_L2,LG_COMP_L_L1,LG_COMP_L_L2,LG_COMP_M_L1,LG_COMP_M_L2,LG_COMP_N_L1,LG_COMP_N_L2,LG_COMP_R_L1,LG_COMP_R_L2,LG_DOOR_C_N_L1,LG_DOOR_C_N_L2,LG_DOOR_C_P_L1,LG_DOOR_C_P_L2,LG_DOOR_L_L1,LG_DOOR_L_L2,LG_DOOR_O_N_L1,LG_DOOR_O_N_L2,LG_DOOR_O_P_L1,LG_DOOR_O_P_L2,LG_DOOR_R_L1,LG_DOOR_R_L2,LG_DR_L_NOS_L1,LG_DR_L_NOS_L2,LG_DR_R_NOS_L1,LG_DR_R_NOS_L2,LG_FAULT_L1,LG_FAULT_L2,LG_FOC_UPLK_L1,LG_FOC_UPLK_L2,LG_IN_CTL_L1,LG_IN_CTL_L2,LG_M_DNLK_L1,LG_M_DNLK_L2,LG_NOT_LOCK_L,LG_NOT_LOCK_N,LG_NOT_LOCK_R,LG_RET_INHI_L1,LG_RET_INHI_L2,LG_SOL_E_N_L1,LG_SOL_E_N_L2,LG_SOL_E_P_L1,LG_SOL_E_P_L2,LG_SOL_R_N_L1,LG_SOL_R_N_L2,LG_SOL_R_P_L1,LG_SOL_R_P_L2,LGC02214,LGCIU_POST_4C,LGCIU1_FAIL,LGCIU1_FAULT,LGCIU1_FLT_4C,LGCIU2_FAIL,LGCIU2_FAULT,LGCIU2_FLT_4C,LH_SSTCK_F,LIFT_GND,LIFT_OFF,LOC_BA,LOC_DEV1,LOC_DEV1_D,LOC_DEV2,LOC_DEV2_D,LOC_DEVC,LOC_GAP,LOC_LAT,LOC_LONG,LOCATION_BA,LONG,LONG_AIR,LONG_POS_CA,LONG_POS_FO,LONG_RES_CA,LONG_RES_FO,LONGC,LONP,LONPC,LOW_ENG_1_S1,LOW_ENG_1_S2,LOW_ENG_2_S1,LOW_ENG_2_S2,LOWSPDT,LS1_MODE_SW,LS1_SEL_MODE,LS2_MODE_SW,LS2_SEL_MODE,MACH,MACH_BUFF,MACH_MAX,MACH_PRESET,MACH_REC,MACH_SEL,MACH_SELON,MACH2,MAIN_GALY_SHED,MAN_PACK1_INOP,MAN_PACK2_INOP,MARKER_INN,MARKER_INNER,MARKER_MID,MARKER_MIDDLE,MARKER_OUT,MARKER_OUTER,MAS_CAU_CA,MAS_CAU_CA_CLR,MAS_CAU_CA_S1,MAS_CAU_CA_S2,MAS_CAU_FO,MAS_CAU_FO_CLR,MAS_CAU_FO_S1,MAS_CAU_FO_S2,MAS_WAR_CA,MAS_WAR_CA_CLR,MAS_WAR_CA_S1,MAS_WAR_CA_S2,MAS_WAR_FO,MAS_WAR_FO_CLR,MAS_WAR_FO_S1,MAS_WAR_FO_S2,MAX_ARMED,MDA_MDH_CA,MDA_MDH_FO,MDA_MDH_SEL,MDA_MDH_SEL_CA,MDA_MDH_SEL_FO,MEMO_MODE_ACT,METRIC_ALT_SEL,MIN_TO_BA,MMO,MODE_ND_CA_CFA,MODE_ND_FO_CFA,MONTH_CURR,MONTH_TO_BA,N1_BA,N1_CMD_1,N1_CMD_2,N1_DISCR_1,N1_DISCR_2,N1_LIMIT_1,N1_LIMIT_2,N1_MIN_BL500,N1_OVERLIM_1,N1_OVERLIM_2,N11,N11_DMC,N11_MAX,N11C,N12,N12_DMC,N12_MAX,N12C,N2_DISCR_1,N2_DISCR_2,N2_OVERLIM_1,N2_OVERLIM_2,N21,N21_DMC,N21_MAX,N21C,N22,N22_DMC,N22_MAX,N22C,NACELLE_TEMP1,NACELLE_TEMP2,NAV_ACCUR_CA,NAV_ACCUR_FO,NAV_ACCUR_HIGH,NAV_MODE,NAV_MODE_CA,NAV_MODE_FO,NAV_STAT_CA_LH,NAV_STAT_CA_RH,NAV_STAT_FO_LH,NAV_STAT_FO_RH,ND_10NM_CA,ND_10NM_FO,ND_160NM_CA,ND_160NM_FO,ND_20NM_CA,ND_20NM_FO,ND_320NM_CA,ND_320NM_FO,ND_40NM_CA,ND_40NM_FO,ND_5NM_CA,ND_5NM_FO,ND_80NM_CA,ND_80NM_FO,ND_ARC_CA,ND_ARC_FO,ND_ARPT_PB_CA,ND_ARPT_PB_FO,ND_CSTR_PB_CA,ND_CSTR_PB_FO,ND_ILS_CA,ND_ILS_FO,ND_ILS_PB_CA,ND_ILS_PB_FO,ND_M_ARC_CA,ND_M_ARC_FO,ND_M_PLAN_CA,ND_M_PLAN_FO,ND_M_RILS_CA,ND_M_RILS_FO,ND_M_RNAV_CA,ND_M_RNAV_FO,ND_M_RVOR_CA,ND_M_RVOR_FO,ND_NAV_CA,ND_NAV_FO,ND_NDB_PB_CA,ND_NDB_PB_FO,ND_PLAN_CA,ND_PLAN_FO,ND_VOR_CA,ND_VOR_FO,ND_VORD_PB_CA,ND_VORD_PB_FO,ND_WPT_PB_CA,ND_WPT_PB_FO,ND1_ANOM_OFF,ND2_ANOM_OFF,NM_RNG_CA_CFA,NM_RNG_FO_CFA,NO_DATA_BMC1,NO_DATA_BMC2,NO_SMOKING_PB,NORM_BRK_FLT,NWS_FAULT,NWS_ORDER_ANG,NWS_ORDER_CA,NWS_ORDER_FO,NWS_SEL_V_FT1,NWS_SEL_V_FT2,NWS_WHEEL_ANG,OANS_AVAIL_ND1,OANS_AVAIL_ND2,OANS_DISP_ND1,OANS_DISP_ND2,OANS_RANGE_CA,OANS_RANGE_FO,OEB_DISCR_FWC,OIL_FILT_CLOG1,OIL_FILT_CLOG2,OIL_HI_TEMP1,OIL_HI_TEMP2,OIL_PRS_1,OIL_PRS_2,OIL_PRS1,OIL_PRS2,OIL_TEMP_1,OIL_TEMP_2,OIL_TMP1,OIL_TMP2,OILPRS1,OILPRS2,OIQ1,OIQ2,OIQDAR1,OIQDAR2,ONE_ENG_APP,OPV_ENG1,OPV_ENG1_R,OPV_ENG2,OPV_ENG2_R,ORIGIN,ORIGIN_BA,OUT_FLOW_V1,OUT_FLOW_V2,OV_HT_MEM_E1,OV_HT_MEM_E2,OV_PRS_MEM_E1,OV_PRS_MEM_E2,OXY_CREW_PB,OXY_CYL_FLT_LT,P125_1,P125_2,P2_1,P2_2,P25_1,P25_2,P49_1,P49_2,PACK_FLOW_PB,PACK_FLOW_R1,PACK_FLOW_R2,PACK_FLOW1,PACK_FLOW2,PACK_HOT_A_PB,PACK_HOT_A_PRV,PACK1_BYPASS_V,PACK1_COMPR_T,PACK1_CTL_FLT,PACK1_DISCH_T,PACK1_FCV_CLS,PACK1_FCV_DISA,PACK1_FCV_DISG,PACK1_FCV_F_C,PACK1_FCV_FC,PACK1_FLOW_INO,PACK1_OVERHEAT,PACK1_PB,PACK1_RAM_I_DR,PACK1_RAM_O_DR,PACK1_T_GT230,PACK1_T_GT260,PACK1_T_GT88,PACK2_BYPASS_V,PACK2_COMPR_T,PACK2_CTL_FLT,PACK2_DISCH_T,PACK2_FCV_CLS,PACK2_FCV_DISA,PACK2_FCV_DISG,PACK2_FCV_F_C,PACK2_FCV_FC,PACK2_FLOW_INO,PACK2_OVERHEAT,PACK2_PB,PACK2_RAM_I_DR,PACK2_RAM_O_DR,PACK2_T_GT230,PACK2_T_GT260,PACK2_T_GT88,PAD,PAR_QUAL,PARK_BRAKE_OFF,PARK_BRK_H_OFF,PASS_NO,PF,PFD_ND_XFR_CA,PFD_ND_XFR_FO,PFD1_ANOM_OFF,PFD2_ANOM_OFF,PHASE1,PILOT_LD,PILOT_TO,PILOT03,PILOTLH1,PILOTLH2,PILOTRH1,PILOTRH2,PIN_BACKUP_SPD,PITCH,PITCH_ALTR,PITCH_ALTR1,PITCH_ALTR2,PITCH_ATT_CA,PITCH_ATT_FO,PITCH_CPT,PITCH_DIR_LAW,PITCH_DIR_LW,PITCH_DISCR_WN,PITCH_DISP_CA,PITCH_DISP_FO,PITCH_FD_FLASH,PITCH_FO,PITCH_MAX_LD,PITCH_MAX_TO,PITCH_NORM_LAW,PITCH_PITCH_WN,PITCH_RATAVGTO,PITCH_RATE,PITCH_RATE1,PITCH_RATMAXTO,PITCH_WARN_CA,PITCH_WARN_FO,PO1_SEL,PO2_SEL,PRECOOL_PRESS1,PRECOOL_PRESS2,PRECOOL_TEMP1,PRECOOL_TEMP2,PRESEL_FQ,PRIORIT_LCK_CA,PRIORIT_LCK_FO,PROBE_WIN_HEAT,PROX_SENSOR1_F,PROX_SENSOR2_F,PRV_ENG1,PRV_ENG1_R,PRV_ENG2,PRV_ENG2_R,PRV1_AUT_C_CTL,PRV1_DISAGREE,PRV1_FO,PRV2_AUT_C_CTL,PRV2_DISAGREE,PRV2_FO,PS3_1,PS3_2,PTU_CTL_V_ON,PTU_FAULT,PTU_PB,PWS_ALERT,PWS_DET_FAULT,PWS_EXT_FAULT,PWS_INT_FAULT,PWS_PB,PWS_PIN_PROG,PWS_WARN,PYLON1_LEAK_MM,PYLON1_LOOP_OP,PYLON2_LEAK_MM,PYLON2_LOOP_OP,QARLOW,QCCU_FAULT,QDBCYC,QDBUPDDA,QDBUPDMO,QNH_ALT_SEL_CA,QNH_ALT_SEL_FO,QNH_AT_DEST,RA_USED_CA,RA_USED_FO,RAD_GPW_MOD_CA,RAD_GPW_MOD_FO,RADIO_XFR_CA,RADIO_XFR_FO,RALT_LH,RALT_RH,RALT1C,RALT1D,RALT2C,RALT2D,RALTC,RAM_AIR_PB,RAT_FULLY_STOW,RAT_NOT_LO_PR,RATE,RAW_ONLY_CA,RAW_ONLY_FO,RECORD,RED_ALT,RED_WARN,REFUSED_TRANS,RELIEF,RETARD_ALT,RETARD_MODE,REV_DEPLOY1,REV_DEPLOY2,REV_IN_LD,REV_POS1,REV_POS2,REV_UNLOCK1,REV_UNLOCK2,RH_SSTCK_F,RNP_PB,ROLL,ROLL_ABS,ROLL_ATT_CA,ROLL_ATT_FO,ROLL_CPT,ROLL_DIR_LAW,ROLL_DISCR_WN,ROLL_DISP_CA,ROLL_DISP_FO,ROLL_FO,ROLL_MAX_AB500,ROLL_MAX_BL100,ROLL_MAX_BL20,ROLL_MAX_BL500,ROLL_NORM_LAW,ROLL_ORDER,ROLL_RATE1,ROP_MAX_BRK,ROP_MAX_REV,ROW_ACT_DRY,ROW_ACT_WET,RTL_LOG_COM,RTL_POS_CA,RTL_POS_FO,RUD_LOG_COM,RUD_PEDA_FORCE,RUD_PEDAL_POS,RUD_POS_DISP,RUD_POS_INPROV,RUD_TRAVEL_CMD,RUD_TRIM_POS,RUDD,RUDDER,RUNWAY_GA,RUNWAY_HDG,RUNWAY_LD,RUNWAY_TO,SAFE_V1_FC,SAFE_V2_FC,SAT,SAT_CA,SAT_FO,SAT_LD,SAT_TO,SB_NUSE,SBL_DIS,SCAVENGE_V_OP1,SCAVENGE_V_OP2,SD_ORIGIN,SDAC1_FAIL,SDAC1_VALID,SDAC2_FAIL,SDAC2_VALID,SDCU_CH1_FAULT,SDCU1_SMK_ALC,SDCU1_SMK_AUC,SDCU1_SMK_FLC,SDCU1_SMK_FUC,SDCU2_SMK_ALC,SDCU2_SMK_AUC,SDCU2_SMK_FLC,SDCU2_SMK_FUC,SDU_CH1_HS,SDU_CH1_LS,SDU_CH2_HS,SDU_CH2_LS,SDU_HS_INSTALL,SEAT_BELT_PB,SEC_PACK1_INOP,SEC_PACK2_INOP,SEC_TO_BA,SEC1_FAULT,SEC1_GSPLR_F,SEC1_PB,SEC1_SBL_F,SEC2_FAULT,SEC2_GSPLR_F,SEC2_PB,SEC2_SBL_F,SEC3_FAULT,SEC3_GSPLR_F,SEC3_PB,SEC3_SBL_F,SF_CNT_I,SF_CNT_O,SF_COUNT,SFCC_DWS_ADMF1,SFCC_DWS_ADMS1,SFCC_DWS_COD2,SFCC_DWS_F133,SFCC_DWS_F193,SFCC_DWS_F243,SFCC_DWS_FACE1,SFCC_DWS_FACE2,SFCC_DWS_FACF1,SFCC_DWS_FAF2,SFCC_DWS_FASF1,SFCC_DWS_FDV1,SFCC_DWS_FDV2,SFCC_DWS_FDV3,SFCC_DWS_FF2,SFCC_DWS_FF3,SFCC_DWS_FFE3,SFCC_DWS_FHSD1,SFCC_DWS_FR3,SFCC_DWS_FRE2,SFCC_DWS_FSJ2,SFCC_DWS_FSJ3,SFCC_DWS_FWE2,SFCC_DWS_FWE3,SFCC_DWS_FWPL1,SFCC_DWS_FWSF1,SFCC_DWS_FXLM1,SFCC_DWS_LDMF1,SFCC_DWS_LH_F1,SFCC_DWS_RH_F1,SFCC_DWS_S173,SFCC_DWS_S263,SFCC_DWS_SALE2,SFCC_DWS_SBE2,SFCC_DWS_SDV1,SFCC_DWS_SDV2,SFCC_DWS_SDV3,SFCC_DWS_SF0_2,SFCC_DWS_SF1_2,SFCC_DWS_SF2,SFCC_DWS_SF2_2,SFCC_DWS_SF3,SFCC_DWS_SF3_2,SFCC_DWS_SFE3,SFCC_DWS_SFF_2,SFCC_DWS_SHSD1,SFCC_DWS_SLID3,SFCC_DWS_SLTD3,SFCC_DWS_SR3,SFCC_DWS_SSJ2,SFCC_DWS_SSJ3,SFCC_DWS_SWE2,SFCC_DWS_SWE3,SFCC_DWS_SWPL1,SFCC_DWS_SWSF1,SFCC_DWS_SXLM1,SI_FLAG_DISP,SIDE_SLIP_DISP,SIDE_SLIP_EST,SIDE_WIND,SIDE_WIND_1,SIDE_WIND_LM,SIDE_WIND_TM,SINGLE_ENG,SLAT,SLAT_C2_FAULT,SLAT_FAULT,SLAT_LVR_CSU_V,SLAT_POS_FWC,SLAT_POS_LH,SLAT_POS_RH,SLAT_SPD,SLAT_SYS1_FLT,SLAT_SYS2_FLT,SLATC,SLATRW,SLOPE,SLPDAR,SMK_AVI_WAR,SMK_BULK_AVN,SMK_CAB_GSM,SMK_CAB_VIDEO,SMK_CRG_WAR,SMK_LVY_WAR,SMK_S_ACB1_F,SMK_S_ACB2_F,SMK_S_ASD_INS,SMK_S_CB1_LOWP,SMK_S_CB2_LOWP,SMK_S_CH2_F,SMK_S_DW_F,SMK_S_EB2_ACT,SMK_S_FCB1_F,SMK_S_FCB2_F,SMK_S_FEB1_INS,SMK_S_FEB2_INS,SMK_S_FSD_INS,SMK_S_LAV_F,SMK_S_LAV_T,SMK_S_LAV_W,SMK_S_UPC_F,SPARE_RED_W_1,SPARE_RED_W_13,SPARE_RED_W_14,SPARE_RED_W_15,SPARE_RED_W_2,SPARE_RED_W_29,SPARE_RED_W_3,SPARE_RED_W_4,SPARE_RED_W_5,SPD_BRK,SPD_BRK_BA,SPD_BRK_POS,SPD_M_CRUISE,SPD_PRESET,SPD_SEL,SPD_SPD_WARN,SPOIL_GND_ARM,SPOIL_LH_1,SPOIL_LH_2,SPOIL_LH_3,SPOIL_LH_4,SPOIL_LH_5,SPOIL_LOUT1,SPOIL_RH_1,SPOIL_RH_2,SPOIL_RH_3,SPOIL_RH_4,SPOIL_RH_5,SPOIL_ROUT1,SPOIL1_AVAIL,SPOIL1_POS_VAL,SPOIL2_AVAIL,SPOIL2_POS_VAL,SPOIL3_AVAIL,SPOIL3_POS_VAL,SPOIL4_AVAIL,SPOIL4_POS_VAL,SPOIL5_AVAIL,SPOIL5_POS_VAL,SPOILER1_F,SPOILER2_F,SPOILER3_F,SPOILER4_F,SPOILER5_F,SST,SSTCK_CPT_INO,SSTCK_FO_INO,STA_VAN_ENG1,STA_VAN_ENG2,STAB,STAB_BLD_POS1,STAB_BLD_POS2,STAB_JAM,STAB_LD,STALL_WAR,STALLSPD,START_ANALYSIS,START_FLIGHT,START_V_ENG1,START_V_ENG1_R,START_V_ENG2,START_V_ENG2_R,STAT_HT_CA_LH,STAT_HT_CA_RH,STAT_HT_FO_LH,STAT_HT_FO_RH,STCK_PRIO_F,STD_ALT_SEL_CA,STD_ALT_SEL_FO,STE_SEL_V_POS1,STE_SEL_V_POS2,STROBE_LT_PB,SURF_WAR,SVA1_CHK_FLT,SVA1_POS,SVA2_CHK_FLT,SVA2_POS,SYNTHESE_CODE,T,T_INL_PRS1,T_INL_PRS2,T_INL_TMP1,T_INL_TMP2,T2_1,T2_2,T25_1,T25_2,T3_1,T3_2,TAIL_WIND,TAIL_WIND_LM,TAIL_WIND_TM,TANK_LH_PUMP1,TANK_LH_PUMP2,TANK_RH_PUMP1,TANK_RH_PUMP2,TANK_TEMP_LH_I,TANK_TEMP_LH_O,TANK_TEMP_RH_I,TANK_TEMP_RH_O,TAS,TAS_REC,TAS1,TAT,TAT_CA,TAT_CRUISE,TAT_FO,TAT_TO,TAT2,TAV_POS_A_C,TAV_POS_C_P,TAV_POS_F_C,TCAS_CMB_CTL,TCAS_CRW_SEL,TCAS_CTL,TCAS_DWN_ADV,TCAS_FAIL,TCAS_FAULT,TCAS_RA_1,TCAS_RA_10,TCAS_RA_11,TCAS_RA_12,TCAS_RA_2,TCAS_RA_3,TCAS_RA_4,TCAS_RA_5,TCAS_RA_6,TCAS_RA_7,TCAS_RA_8,TCAS_RA_9,TCAS_RA_INHIB,TCAS_RA_NUM,TCAS_RAC,TCAS_SENS_1,TCAS_SENS_2,TCAS_SENS_3,TCAS_STANDBY,TCAS_TA,TCAS_TA_1,TCAS_TA_2,TCAS_TA_ONLY,TCAS_UP_ADV,TCAS_VALID,TCAS_VRT_CTL,TEMP_AFTER_CAB,TEMP_AT_DEST,TEMP_COCKPIT,TEMP_FORE_CAB,TEMP_SEL_A_C,TEMP_SEL_A_CGO,TEMP_SEL_C_P,TEMP_SEL_F_C,TGT_SIDE_SLIP,TGT_SPD_COLOR,THR_ABV_IDLE1,THR_ABV_IDLE2,THR_DEFICIT,THR_EPR_MODE,THR1_LOCKED,THR2_LOCKED,THRUST_EPR,THS_MAN,THS_MAN_SW,TIME,TIME_1000,TIME_BA,TIME_ENG_START,TIME_ENG_STOP,TIME_IDLE_OFF,TIME_LD,TIME_PARK,TIME_R,TIME_STAMP,TIME_START_TO,TIME_TAG,TIME_TAXI_IN,TIME_TAXI_OUT,TIME_TO,TIME_TO_RUNWY,TIME_TOC,TIME_TOD,TK_C_L_LO_LVL,TK_C_PUMP1_LP,TK_C_PUMP1_ON,TK_C_PUMP1_PB,TK_C_PUMP2_LP,TK_C_PUMP2_ON,TK_C_PUMP2_PB,TK_C_R_LO_LVL,TK_C_XFR_CV1FC,TK_C_XFR_CV1FO,TK_C_XFR_CV2FC,TK_C_XFR_CV2FO,TK_C_XFR_LH_PB,TK_C_XFR_RH_PB,TK_L_PUMP1_LP,TK_L_PUMP2_LP,TK_R_PUMP1_LP,TK_R_PUMP2_LP,TLA_BELOW_CLB,TLA1,TLA1C,TLA1DMC,TLA2,TLA2C,TLA2DMC,TLU_EGD_CA,TLU_EGD_FO,TO_CG,TO_CONF_SIDE_S,TO_THR_DISAGRE,TO_TLA1,TO_TLA2,TO_WAYPT_1_4,TO_WAYPT_5_7,TO_WAYPT_CA1,TO_WAYPT_CA2,TO_WAYPT_CA3,TO_WAYPT_CA4,TO_WAYPT_CA5,TO_WAYPT_CA6,TO_WAYPT_CA7,TO_WAYPT_CA8,TO_WAYPT_CA9,TO_WAYPT_FO1,TO_WAYPT_FO2,TO_WAYPT_FO3,TO_WAYPT_FO4,TO_WAYPT_FO5,TO_WAYPT_FO6,TO_WAYPT_FO7,TO_WAYPT_FO8,TO_WAYPT_FO9,TO1,TO2,TO3,TO4,TOUCH_DOWN,TOUCH_GND,TOWING_ON,TR1_CURRENT,TR1_VOLTAGE,TR2_CURRENT,TR2_VOLTAGE,TRA_CHK_FAIL1,TRA_CHK_FAIL2,TRACK_SEL,TRB1_COOL,TRB2_COOL,TRIGGER_CODE,TRK_FPA_SEL,TRKANGTR,TRU1_CNTOR,TRU2_CNTOR,TSS_DISP,TT25_L,TT25_R,TTDWN,TURBULENCE,TYRE_PR_M1,TYRE_PR_M2,TYRE_PR_M3,TYRE_PR_M4,TYRE_PR_N1,TYRE_PR_N2,UP_STM_ISO_V_C,UTC_HOUR,UTC_MIN,UTC_MIN_SEC,UTC_SEC,V1,V1_REC,V2,V2_MIN,V2_REC,V2_TO,V3,V4,VALUE_ST,VAPP,VAPP_FMS,VAPP_LD,VAT1,VB11,VB11_MAX,VB12,VB12_MAX,VB21,VB21_MAX,VB22,VB22_MAX,VCTRENDS,VDEV_DISP_CA,VDEV_DISP_FO,VERT_DEV,VERT_DEV_CA,VERT_DEV_FO,VEV,VFE,VGDOT,VHF1,VHF2,VHF3,VIB_N1_BRG1,VIB_N1_BRG2,VIB_N11_ADV,VIB_N12_ADV,VIB_N2_BRG1,VIB_N2_BRG2,VIB_N21_ADV,VIB_N22_ADV,VIDEO_MSG_SD,VIDEO_ON_SD,VLS,VLS_REC,VMAN,VMAX,VMAX_REC,VMO,VMO_MMO_OVS,VOR_FRQ1,VOR_FRQ2,VOR_MODE_ARM,VOR_MODE_DIS,VOR_MODE_EGD,VOR_SEL_CRS1,VOR_SEL_CRS2,VOR1_BRG,VOR1_BRG_SW_CA,VOR1_BRG_SW_FO,VOR1_TUNE_MODE,VOR2_BRG,VOR2_BRG_SW_CA,VOR2_BRG_SW_FO,VOR2_TUNE_MODE,VR,VREF,VRTG,VRTG_AVG,VRTG_MAX_AIR,VRTG_MAX_GND,VRTG_MAX_LD,VS1G,VSCB1,VSCB2,VSW,WEA_ALT,WEA_DEWPOINT,WEA_TEMP,WEA_VALID,WEA_VISIB,WEA_WINDIR,WEA_WINSPD,WHEEL_SPD1,WHEEL_SPD2,WHEEL_SPD3,WHEEL_SPD4,WIN_DIR,WIN_DIR_DMC,WIN_SPD,WIN_SPD_DMC,WIN_SPDR,WIND_SH_HT_L_F,WIND_SH_HT_R_F,WINDOW_HT_LH_F,WINDOW_HT_RH_F,WING_A_I_SYSON,WING_L_A_I_HP,WING_L_A_I_LP,WING_L_A_I_V_C,WING_L_AI_V_C,WING_L_LO_LV_A,WING_L_LO_LV_B,WING_LH_OVFLOW,WING_R_A_I_HP,WING_R_A_I_LP,WING_R_A_I_V_C,WING_R_AI_V_C,WING_R_LO_LV_A,WING_R_LO_LV_B,WING_RH_OVFLOW,WING1_LEAK_MEM,WING1_LOOPA_OP,WING1_LOOPB_OP,WING2_LEAK_MEM,WING2_LOOPA_OP,WING2_LOOPB_OP,WXR_ND_FLT_CA,WXR_ND_FLT_FO,WXR_PFD_FLT_CA,WXR_PFD_FLT_FO,WXR_TERR_CA,WXR_TERR_FO,WXR_VALID_CA,WXR_VALID_FO,X_AIR,XFEED_V_A_C_D,XFEED_V_A_O_D,XFEED_V_AUT_CT,XFEED_V_FC,XFEED_V_FC1,XFEED_V_FC2,XFEED_V_FO,XFEED_V_M_C_D,XFEED_V_M_O_D,XFEED_V_SEL_O,XFEED_V_SEL_S,XFR_CNTOR1_BTC,XFR_CNTOR2_BTC,XLS_FLAG_CA,XLS_FLAG_FO,Y_ACCU_LO_PR,Y_AIR,Y_E2_PUMP_LP,Y_E2_PUMP_PB,Y_ELE_PMP_OVHT,Y_ELEC_PUMP_ON,Y_ELEC_PUMP_PB,Y_RSVR_LO_A_PR,Y_RSVR_LO_LVL,Y_RSVR_OV_HEAT,YAW,YAW_LOG_COM,YD_ORDER,YD1_FAULT,YD2_FAULT,YEAR_CURR,YEAR_MED,YEAR_TO_BA,ZC_DSW_061,ZC_DSW_AC2_F,ZC_DSW_ACSC_IS,ZC_DSW_AL1_ACT,ZC_DSW_AL1_INO,ZC_DSW_AL2_INO,ZC_DSW_GTFAN,ZC_DSW_HA_SW,ZC_DSW_LHFAN,ZC_DSW_OH_AC_W,ZC_DSW_OH_CP_W,ZC_DSW_OH_FC_W,ZC_DSW_RHFAN,ZC_DSW_TAOP,ZC_DSW_TAP_CLS,ZC_DSW_TAS_INO,ZC_DSW_TP_DIS,ZFCG,ZFW,ZLD1,ZLD2";

    public static void main(String s[]){

//        checkUserColumn();


        try {
            readHDFSData();
        } catch (IOException e){
            System.out.printf(e.toString());
        }
    }

    private static void readHDFSData() throws IOException {
        config = new Configuration();
        String gzipCodecClassName = "org.apache.hadoop.io.compress.GzipCodec";
        CompressionCodec codec = null;
        try {
            Class<?> codeClass = Class.forName(gzipCodecClassName); //解压缩类
            codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, config); //创建解压缩类的对象
        } catch (ClassNotFoundException e){
            System.out.println(e.toString());
        }


        HTable table = creatHbaseTable(); //创建Hbase表

        String line = "";

        //flightId_1.gzip
        for (int i = 1; i < 100; i ++){
            String url = "hdfs://192.168.137.102:8020/data/dh/flightId_" + i +".gzip";

            FileSystem fs = FileSystem.get(URI.create(url),config);
            FSDataInputStream inputStream = fs.open(new Path(url));

            BufferedReader reader = new BufferedReader(new InputStreamReader(codec.createInputStream(inputStream)));

            int rowNumber = 0;
            ArrayList<Put> puts = new ArrayList<>();
            while ((line = reader.readLine()) != null){
                rowNumber++;
                if (rowNumber == 0)
                    continue;
                puts.add(creatNewPut(rowNumber+"", line, table));
                System.out.println("*****************" + rowNumber + "行读取完毕");
            }
            System.out.println("//////////////////////////////////第" + i + "次，开始向表中存数据");
            table.put(puts);
            System.out.println("==========================" + i + "个文件存储完毕");
        }
    }

    /**
     * 创建Hbase表
     * @throws IOException
     */
    private static HTable creatHbaseTable() throws IOException{
        config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", "192.168.137.101,192.168.137.102,192.168.137.103");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection conn = ConnectionFactory.createConnection(config);

        HBaseAdmin hBaseAdmin = (HBaseAdmin)conn.getAdmin();

        //先删除再添加
        if (hBaseAdmin.tableExists(TABLE_NAME)){
            hBaseAdmin.disableTable(TABLE_NAME);
            hBaseAdmin.deleteTable(TABLE_NAME);
            System.out.println("==========================表" + TABLE_NAME + "删除完毕");
        }

        HTableDescriptor td = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        td.addFamily(new HColumnDescriptor(CF_1_NAME));
        td.addFamily(new HColumnDescriptor(CF_2_NAME));

        hBaseAdmin.createTable(td);

        System.out.println("==========================表" + TABLE_NAME + "创建完毕");

        return (HTable) conn.getTable(TableName.valueOf(TABLE_NAME));
    }

    /**
     * 使用到的Column的索引为0,117,618,2504
     * @param lineNumber
     * @param lineContent
     * @throws IOException
     */
    private static Put creatNewPut(String lineNumber, String lineContent, HTable table) throws IOException {

        String[] cells = lineContent.split(",");
        String[] columnNames = COLUMN_NAMES.replace("-", "_").split(",");
        Put put = new Put((lineNumber + cells[0]).getBytes());

        for (int i = 0; i < cells.length; i ++){
            String cell = cells[i];
            if (i == 0 || i == 1 || i == 117 || i == 618 || i == 1448 || i == 2131)
                put.addColumn(CF_1_NAME.getBytes(), columnNames[i].getBytes(), cell.getBytes());
            else
                put.addColumn(CF_2_NAME.getBytes(), columnNames[i].getBytes(), cell.getBytes());
        }
        return put;
    }


    /**
     * 获取需要放进使用列族的Column索引
     * 找到了 0     1    117      618     1448     2131
     */
    private static void checkUserColumn(){
        String[] names = COLUMN_NAMES.replace("-", "_").split(",");
        for (int i = 0; i < names.length; i++){
            String name = names[i];
            if (name.equals("Time") || name.equals("ALT_QNH") || name.equals("HEIGHT") || name.equals("RALTC") || name.equals("DATE_R") || name.equals("Flight_ID"))
                System.out.println("第" + i + "个字段");
        }
    }

}