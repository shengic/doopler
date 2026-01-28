/*
 Navicat Premium Data Transfer

 Source Server         : mysql80-5090
 Source Server Type    : MySQL
 Source Server Version : 80042 (8.0.42)
 Source Host           : localhost:3306
 Source Schema         : doopler

 Target Server Type    : MySQL
 Target Server Version : 80042 (8.0.42)
 File Encoding         : 65001

 Date: 28/01/2026 17:43:07
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for vad_gate_fit
-- ----------------------------
DROP TABLE IF EXISTS `vad_gate_fit`;
CREATE TABLE `vad_gate_fit`  (
  `run_id` bigint UNSIGNED NOT NULL COMMENT '對應 proc_run.run_id；不同規則/批次分版',
  `header_id` bigint UNSIGNED NOT NULL COMMENT '掃描批次/檔頭 ID',
  `range_gate_index` int UNSIGNED NOT NULL COMMENT '掃描內的 range gate 索引（0 或 1-based 依系統）',
  `n_total_rays` int UNSIGNED NULL DEFAULT NULL COMMENT '此 gate 的總 ray 數（含未通過 QC）',
  `n_selected_rays` int UNSIGNED NULL DEFAULT NULL COMMENT '最終參與解算的 ray 數（qc_selected=1，上限通常 6）',
  `u_ms` double NULL DEFAULT NULL COMMENT '東向風速（+向東）',
  `v_ms` double NULL DEFAULT NULL COMMENT '北向風速（+向北）',
  `w_ms` double NULL DEFAULT NULL COMMENT '垂直風速（+向上）',
  `speed_ms` double NULL DEFAULT NULL COMMENT '水平風速 = hypot(u,v)',
  `speed_total_ms` double NULL DEFAULT NULL COMMENT 'total speed sqrt(u^2+v^2+w^2)',
  `dir_deg` decimal(6, 3) NULL DEFAULT NULL COMMENT '風向（來向；北=0，順時針）',
  `r2` double NULL DEFAULT NULL COMMENT '決定係數 R²',
  `rmse_ms` double NULL DEFAULT NULL COMMENT '殘差均方根（m/s）',
  `status` enum('ok','insufficient_samples','no_elevation','solve_fail') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT 'ok' COMMENT 'ok=成功；insufficient_samples=<3；no_elevation=缺 elev；solve_fail=數值失敗',
  `selected_ray_idx_csv` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '參與解算的 ray_idx（逗號分隔）',
  `selected_azimuth_deg_csv` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '參與解算的 azimuth (deg)',
  `selected_elevation_deg_csv` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '參與解算的 elevation (deg)',
  `svd_singular_values` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'A 矩陣 SVD 奇異值（逗號分隔）',
  `cond_num` double NULL DEFAULT NULL COMMENT '條件數 = smax/smin（由 SVD 估）',
  `a_rank` int NULL DEFAULT NULL COMMENT 'np.linalg.lstsq 回報的秩（理想=3）',
  `az_span_deg` double NULL DEFAULT NULL COMMENT '選取樣本的方位角最小覆蓋角度（deg）',
  `warn_flags` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '逗號旗標：ILLCOND,LOWSPAN,LOWR2,LOWRANK',
  `rule_tag` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '規則簽章/名稱/雜湊（來自 qc_tagging_v2.py）',
  `code_version` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT 'wind_profile_uvw_v1.3.0' COMMENT '寫入此列的程式版本',
  `params_json` json NULL COMMENT '本次流程參數（max_selected、閾值等 JSON）',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`run_id`, `header_id`, `range_gate_index`) USING BTREE,
  INDEX `idx_fit_rule`(`rule_tag` ASC) USING BTREE,
  INDEX `idx_fit_status`(`status` ASC) USING BTREE,
  INDEX `idx_fit_hdr_gate`(`header_id` ASC, `range_gate_index` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = 'VAD 反演結果（m/s / deg；dir為氣象來向），含病態/覆蓋度診斷與運行版本資訊' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
