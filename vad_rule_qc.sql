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

 Date: 21/01/2026 14:30:31
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

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
-- Records of vad_rule_qc
-- ----------------------------
INSERT INTO `vad_rule_qc` VALUES (1, 'check_nulls', 'nulls', 'required fields must be present', 0, 10);
INSERT INTO `vad_rule_qc` VALUES (2, 'check_snr_min', 'snr_min', '(intensity_snr_plus1 - 1) >= SNR_MIN (SNR_MIN=0.015; linear ≈ -18.2 dB)', 1, 20);
INSERT INTO `vad_rule_qc` VALUES (3, 'check_spectral_width_max', 'sw_max', 'spectral_width_ms <= K_SW * instrument_spectral_width_ms (K_SW=1.5)', 1, 30);
INSERT INTO `vad_rule_qc` VALUES (4, 'check_pitch_roll_max', 'tilt_abs_max', 'max(|pitch|,|roll|) <= TILT_ABS_MAX (2.0°)', 0, 40);
INSERT INTO `vad_rule_qc` VALUES (5, 'check_attitude_vector', 'tilt_rss_max', 'sqrt(pitch^2 + roll^2) <= TILT_RSS_MAX (2.5°)', 0, 50);
INSERT INTO `vad_rule_qc` VALUES (6, 'check_elevation_range', 'elev_range', 'ELEV_MIN <= elevation_deg <= ELEV_MAX (10–20°)', 0, 60);
INSERT INTO `vad_rule_qc` VALUES (7, 'check_azimuth_duplicate_guard', 'az_dup_guard', 'azimuth not within ±AZ_DUP_TOL of a seen az (AZ_DUP_TOL=0.1°, 0°~360° collapsed)', 0, 70);
INSERT INTO `vad_rule_qc` VALUES (8, 'check_velocity_bounds', 'vr_bounds', '|doppler_ms| <= VR_ABS_MAX (60 m/s)', 0, 80);
INSERT INTO `vad_rule_qc` VALUES (21, 'check_gate_outlier_mad', 'gate_mad', '|vr - median|/(1.4826*MAD) <= MAD_K (MAD_K=3.5) per gate', 0, 120);
INSERT INTO `vad_rule_qc` VALUES (22, 'check_azimuth_coverage_gate', 'gate_coverage', 'unique snapped az count >= MIN_RAYS and span >= MIN_SPAN_DEG (6 rays, 240°)', 0, 130);
INSERT INTO `vad_rule_qc` VALUES (23, 'check_vertical_consistency', 'vertical_consistency', '|median(gi) - avg(neighbor medians)| <= VERT_THR (1.2 m/s, neighbors ±1 gate)', 0, 140);
INSERT INTO `vad_rule_qc` VALUES (24, 'check_gate_uniform_bin_fill', 'gate_bin_fill', 'nonempty azimuth bins >= MIN_NONEMPTY_BINS (bin=10°, need >=6)', 0, 150);

SET FOREIGN_KEY_CHECKS = 1;
