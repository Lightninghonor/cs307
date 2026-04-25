-- 高级功能补充表：登录认证 + 联系人
-- 在 02_booking_schema.sql 执行完毕后运行

SET search_path TO flightdb, public;

-- 为 passenger 表添加密码字段（bcrypt hash）
ALTER TABLE passenger
    ADD COLUMN IF NOT EXISTS password_hash text;

-- 联系人表：乘客可添加多个联系人，可为联系人订票
CREATE TABLE IF NOT EXISTS contact (
    contact_id   int generated always as identity primary key,
    owner_id     int not null references passenger(passenger_id) on update cascade on delete cascade,
    name         text not null,
    mobile_number varchar(20) not null,
    constraint ck_contact_mobile check (mobile_number ~ '^[0-9]{7,20}$')
);

CREATE INDEX IF NOT EXISTS idx_contact_owner ON contact(owner_id);
