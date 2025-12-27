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

 Date: 27/12/2025 09:36:01
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for import_run
-- ----------------------------
DROP TABLE IF EXISTS `import_run`;
CREATE TABLE `import_run`  (
  `import_id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `folder_path` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT '匯入的資料夾路徑',
  `files_count` int UNSIGNED NULL DEFAULT 0 COMMENT '該次匯入的檔案總數',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`import_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for proc_run
-- ----------------------------
DROP TABLE IF EXISTS `proc_run`;
CREATE TABLE `proc_run`  (
  `run_id` bigint UNSIGNED NOT NULL,
  `rule_tag` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `started_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `finished_at` datetime NULL DEFAULT NULL,
  `params_json` json NULL,
  PRIMARY KEY (`run_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

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

-- ----------------------------
-- Table structure for vad_rule_qc
-- ----------------------------
DROP TABLE IF EXISTS `vad_rule_qc`;
CREATE TABLE `vad_rule_qc`  (
  `rule_id` int NOT NULL,
  `def_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `rule_code` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `description` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `rule_order` int NULL DEFAULT 0,
  PRIMARY KEY (`rule_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for wind_profile_gate
-- ----------------------------
DROP TABLE IF EXISTS `wind_profile_gate`;
CREATE TABLE `wind_profile_gate`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `header_id` bigint UNSIGNED NOT NULL,
  `ray_idx` smallint UNSIGNED NOT NULL,
  `range_gate_index` smallint UNSIGNED NOT NULL,
  `doppler_ms` double NULL DEFAULT NULL,
  `intensity_snr_plus1` double NULL DEFAULT NULL,
  `beta_m_inv_sr_inv` double NULL DEFAULT NULL,
  `spectral_width_ms` double NULL DEFAULT NULL,
  `decimal_time_hours` double NULL DEFAULT NULL,
  `azimuth_deg` double NULL DEFAULT NULL,
  `elevation_deg` double NULL DEFAULT NULL,
  `pitch_deg` double NULL DEFAULT NULL,
  `roll_deg` double NULL DEFAULT NULL,
  `center_of_gate` double NULL DEFAULT NULL,
  `qc_selected` tinyint(1) NOT NULL DEFAULT 0,
  `qc_failed_rules_csv` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `qc_failed_rule_count` tinyint UNSIGNED NOT NULL DEFAULT 0,
  `qc_updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uq_gate_triplet`(`header_id` ASC, `ray_idx` ASC, `range_gate_index` ASC) USING BTREE,
  INDEX `idx_header_ray`(`header_id` ASC, `ray_idx` ASC) USING BTREE,
  INDEX `idx_header_gate`(`header_id` ASC, `range_gate_index` ASC) USING BTREE,
  INDEX `idx_qc_hdr_sel`(`header_id` ASC, `qc_selected` ASC) USING BTREE,
  CONSTRAINT `fk_gate_header` FOREIGN KEY (`header_id`) REFERENCES `wind_profile_header` (`header_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 621712 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for wind_profile_gate_copy1
-- ----------------------------
DROP TABLE IF EXISTS `wind_profile_gate_copy1`;
CREATE TABLE `wind_profile_gate_copy1`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `header_id` bigint UNSIGNED NOT NULL,
  `ray_idx` smallint UNSIGNED NOT NULL,
  `range_gate_index` smallint UNSIGNED NOT NULL,
  `doppler_ms` double NULL DEFAULT NULL,
  `intensity_snr_plus1` double NULL DEFAULT NULL,
  `beta_m_inv_sr_inv` double NULL DEFAULT NULL,
  `spectral_width_ms` double NULL DEFAULT NULL,
  `decimal_time_hours` double NULL DEFAULT NULL,
  `azimuth_deg` double NULL DEFAULT NULL,
  `elevation_deg` double NULL DEFAULT NULL,
  `pitch_deg` double NULL DEFAULT NULL,
  `roll_deg` double NULL DEFAULT NULL,
  `center_of_gate` double NULL DEFAULT NULL,
  `qc_selected` tinyint(1) NOT NULL DEFAULT 0,
  `qc_failed_rules_csv` varchar(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `qc_failed_rule_count` tinyint UNSIGNED NOT NULL DEFAULT 0,
  `qc_updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uq_gate_triplet`(`header_id` ASC, `ray_idx` ASC, `range_gate_index` ASC) USING BTREE,
  INDEX `idx_header_ray`(`header_id` ASC, `ray_idx` ASC) USING BTREE,
  INDEX `idx_header_gate`(`header_id` ASC, `range_gate_index` ASC) USING BTREE,
  INDEX `idx_qc_hdr_sel`(`header_id` ASC, `qc_selected` ASC) USING BTREE,
  CONSTRAINT `wind_profile_gate_copy1_ibfk_1` FOREIGN KEY (`header_id`) REFERENCES `wind_profile_header` (`header_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 621712 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for wind_profile_header
-- ----------------------------
DROP TABLE IF EXISTS `wind_profile_header`;
CREATE TABLE `wind_profile_header`  (
  `header_id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `import_id` bigint UNSIGNED NULL DEFAULT NULL,
  `filename` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `system_id` smallint UNSIGNED NULL DEFAULT NULL,
  `num_gates` smallint UNSIGNED NULL DEFAULT NULL,
  `range_gate_length_m` double NULL DEFAULT NULL,
  `gate_length_pts` smallint UNSIGNED NULL DEFAULT NULL,
  `pulses_per_ray` int UNSIGNED NULL DEFAULT NULL,
  `num_rays_in_file` smallint UNSIGNED NULL DEFAULT NULL,
  `scan_type` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `focus_range` int UNSIGNED NULL DEFAULT NULL,
  `start_time` datetime(2) NULL DEFAULT NULL,
  `velocity_resolution_ms` double NULL DEFAULT NULL,
  `range_center_formula` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `data_line1_format` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `data_line2_format` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,
  `instrument_spectral_width_ms` double NULL DEFAULT NULL,
  PRIMARY KEY (`header_id`) USING BTREE,
  UNIQUE INDEX `uq_file_time`(`filename` ASC, `start_time` ASC) USING BTREE,
  INDEX `fk_header_import`(`import_id` ASC) USING BTREE,
  CONSTRAINT `fk_header_import` FOREIGN KEY (`import_id`) REFERENCES `import_run` (`import_id`) ON DELETE CASCADE ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 313 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Triggers structure for table import_run
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_cleanup_import_data`;
delimiter ;;
CREATE TRIGGER `trg_cleanup_import_data` BEFORE DELETE ON `import_run` FOR EACH ROW BEGIN
    -- 1. Explicitly delete headers (this will trigger gate deletion via its own cascade)
    -- This is safer than deleting from gate directly because it maintains the hierarchy
    DELETE FROM wind_profile_header 
    WHERE import_id = OLD.import_id;

    -- 2. Optional: Clean up any audit logs or specific calculation results
    -- Since vad_gate_fit is tied to header_id, it is already handled if you have 
    -- the header-level triggers we discussed earlier.
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table proc_run
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_cascade_delete_run`;
delimiter ;;
CREATE TRIGGER `trg_cascade_delete_run` AFTER DELETE ON `proc_run` FOR EACH ROW BEGIN
    -- 當 proc_run 的一筆資料被刪除後，自動刪除 vad_gate_fit 對應的資料
    DELETE FROM vad_gate_fit 
    WHERE run_id = OLD.run_id;
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table wind_profile_gate
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_wpg_bi_center`;
delimiter ;;
CREATE TRIGGER `trg_wpg_bi_center` BEFORE INSERT ON `wind_profile_gate` FOR EACH ROW BEGIN
  DECLARE gate_len DOUBLE DEFAULT NULL;
  SELECT h.range_gate_length_m INTO gate_len
  FROM doopler.wind_profile_header h
  WHERE h.header_id = NEW.header_id;

  SET NEW.center_of_gate =
      (NEW.range_gate_index + 0.5) * COALESCE(gate_len, 0);
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table wind_profile_gate
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_wpg_bu_center`;
delimiter ;;
CREATE TRIGGER `trg_wpg_bu_center` BEFORE UPDATE ON `wind_profile_gate` FOR EACH ROW BEGIN
  DECLARE gate_len DOUBLE DEFAULT NULL;

  IF NEW.range_gate_index <> OLD.range_gate_index
     OR NEW.header_id <> OLD.header_id THEN

    SELECT h.range_gate_length_m INTO gate_len
    FROM doopler.wind_profile_header h
    WHERE h.header_id = NEW.header_id;

    SET NEW.center_of_gate =
        (NEW.range_gate_index + 0.5) * COALESCE(gate_len, 0);
  END IF;
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table wind_profile_gate_copy1
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_wpg_bi_center_copy1`;
delimiter ;;
CREATE TRIGGER `trg_wpg_bi_center_copy1` BEFORE INSERT ON `wind_profile_gate_copy1` FOR EACH ROW BEGIN
  DECLARE gate_len DOUBLE DEFAULT NULL;
  SELECT h.range_gate_length_m INTO gate_len
  FROM doopler.wind_profile_header h
  WHERE h.header_id = NEW.header_id;

  SET NEW.center_of_gate =
      (NEW.range_gate_index + 0.5) * COALESCE(gate_len, 0);
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table wind_profile_gate_copy1
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_wpg_bu_center_copy1`;
delimiter ;;
CREATE TRIGGER `trg_wpg_bu_center_copy1` BEFORE UPDATE ON `wind_profile_gate_copy1` FOR EACH ROW BEGIN
  DECLARE gate_len DOUBLE DEFAULT NULL;

  IF NEW.range_gate_index <> OLD.range_gate_index
     OR NEW.header_id <> OLD.header_id THEN

    SELECT h.range_gate_length_m INTO gate_len
    FROM doopler.wind_profile_header h
    WHERE h.header_id = NEW.header_id;

    SET NEW.center_of_gate =
        (NEW.range_gate_index + 0.5) * COALESCE(gate_len, 0);
  END IF;
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table wind_profile_header
-- ----------------------------
DROP TRIGGER IF EXISTS `trg_cleanup_vad_results`;
delimiter ;;
CREATE TRIGGER `trg_cleanup_vad_results` BEFORE DELETE ON `wind_profile_header` FOR EACH ROW BEGIN
    -- Delete any calculation results tied to this specific header
    DELETE FROM vad_gate_fit 
    WHERE header_id = OLD.header_id;
END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
