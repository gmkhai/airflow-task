-- This script only contains the table creation statements and does not fully represent the table in the database. Do not use it as a backup.

-- Table Definition
CREATE TABLE "public"."event" (
    "event_id" int8 NOT NULL,
    "event_name" varchar(100) NOT NULL,
    "event_bundle_sequence_id" varchar(5) NOT NULL,
    "event_user_pseudo_count" int8 NOT NULL,
    PRIMARY KEY ("event_id")
);

INSERT INTO "public"."event" ("event_id", "event_name", "event_bundle_sequence_id", "event_user_pseudo_count") VALUES
(0, 'first_open', '1', 4);
INSERT INTO "public"."event" ("event_id", "event_name", "event_bundle_sequence_id", "event_user_pseudo_count") VALUES
(1, 'os_update', '15', 1);
INSERT INTO "public"."event" ("event_id", "event_name", "event_bundle_sequence_id", "event_user_pseudo_count") VALUES
(2, 'os_update', '3', 1);
INSERT INTO "public"."event" ("event_id", "event_name", "event_bundle_sequence_id", "event_user_pseudo_count") VALUES
(3, 'os_update', '4', 1),
(4, 'screen_view', '10', 2),
(5, 'screen_view', '11', 3),
(6, 'screen_view', '12', 10),
(7, 'screen_view', '13', 1),
(8, 'screen_view', '15', 2),
(9, 'screen_view', '16', 1),
(10, 'screen_view', '2', 12),
(11, 'screen_view', '26', 2),
(12, 'screen_view', '27', 3),
(13, 'screen_view', '3', 7),
(14, 'screen_view', '4', 10),
(15, 'screen_view', '44', 2),
(16, 'screen_view', '45', 3),
(17, 'screen_view', '46', 5),
(18, 'screen_view', '47', 2),
(19, 'screen_view', '5', 16),
(20, 'screen_view', '6', 33),
(21, 'screen_view', '60', 2),
(22, 'screen_view', '61', 3),
(23, 'screen_view', '62', 2),
(24, 'screen_view', '69', 4),
(25, 'screen_view', '7', 18),
(26, 'screen_view', '70', 2),
(27, 'screen_view', '71', 1),
(28, 'select_content', '12', 2),
(29, 'select_content', '27', 1),
(30, 'select_content', '45', 1),
(31, 'select_content', '46', 2),
(32, 'select_content', '5', 4),
(33, 'select_content', '6', 3),
(34, 'select_content', '62', 1),
(35, 'select_content', '7', 4),
(36, 'select_content', '70', 1),
(37, 'session_start', '10', 1),
(38, 'session_start', '11', 1),
(39, 'session_start', '12', 1),
(40, 'session_start', '15', 1),
(41, 'session_start', '2', 4),
(42, 'session_start', '26', 1),
(43, 'session_start', '3', 1),
(44, 'session_start', '4', 2),
(45, 'session_start', '44', 1),
(46, 'session_start', '60', 1),
(47, 'session_start', '69', 1),
(48, 'session_start', '70', 1),
(49, 'session_start', '8', 1),
(50, 'user_engagement', '11', 1),
(51, 'user_engagement', '12', 5),
(52, 'user_engagement', '13', 1),
(53, 'user_engagement', '16', 1),
(54, 'user_engagement', '2', 7),
(55, 'user_engagement', '27', 2),
(56, 'user_engagement', '3', 5),
(57, 'user_engagement', '4', 7),
(58, 'user_engagement', '45', 1),
(59, 'user_engagement', '46', 4),
(60, 'user_engagement', '47', 2),
(61, 'user_engagement', '5', 12),
(62, 'user_engagement', '6', 19),
(63, 'user_engagement', '61', 1),
(64, 'user_engagement', '62', 2),
(65, 'user_engagement', '69', 3),
(66, 'user_engagement', '7', 10),
(67, 'user_engagement', '72', 1),
(68, 'user_engagement', '8', 1);