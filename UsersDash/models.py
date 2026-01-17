# models.py
# Модели БД: пользователи, сервера, аккаунты, доп. данные по фермам,
# снапшоты ресурсов и лог действий.

from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin

db = SQLAlchemy()


class User(UserMixin, db.Model):
    """
    Пользователи системы (как админы, так и клиенты).
    """
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(16), nullable=False, default="client")  # 'admin' или 'client'
    is_active = db.Column(db.Boolean, default=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login_at = db.Column(db.DateTime)

    # Один пользователь -> много аккаунтов
    accounts = db.relationship(
        "Account",
        back_populates="owner",
        lazy="dynamic",
    )
    telegram_profile = db.relationship(
        "TelegramSubscriber",
        back_populates="owner",
        uselist=False,
    )

    def get_id(self):
        """
        Flask-Login использует этот метод для идентификации пользователя.
        """
        return str(self.id)

    def __repr__(self):
        return f"<User {self.username} ({self.role})>"


class Server(db.Model):
    """
    Серверный ПК (F99, 208, DELL и т.д.), на котором крутятся фермы.
    """
    __tablename__ = "servers"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True, nullable=False, index=True)
    host = db.Column(db.String(255), nullable=False)         # IP или DNS
    api_base_url = db.Column(db.String(255), nullable=True)  # URL к локальному API (RssCounter)
    api_token = db.Column(db.String(255), nullable=True)     # Токен доступа к REST API
    description = db.Column(db.String(255), nullable=True)
    is_active = db.Column(db.Boolean, default=True)

    # Один сервер -> много аккаунтов
    accounts = db.relationship(
        "Account",
        back_populates="server",
        lazy="dynamic",
    )

    def __repr__(self):
        return f"<Server {self.name} ({self.host})>"



class Account(db.Model):
    __tablename__ = "accounts"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)

    server_id = db.Column(db.Integer, db.ForeignKey("servers.id"), nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    # GUID из /api/resources (поле id) — можем не заполнять, тогда маппим по имени
    internal_id = db.Column(db.String(128), nullable=True, unique=True, index=True)

    # Активна ли ферма
    is_active = db.Column(db.Boolean, default=True, nullable=False)

    # Блокировка за неоплату (клиенты не могут включить сами)
    blocked_for_payment = db.Column(db.Boolean, default=False, nullable=False)

    # Игровое королевство (мир) и заметки по ферме
    game_world = db.Column(db.Text, nullable=True)
    notes = db.Column(db.Text, nullable=True)

    # Дата и сумма ближайшей оплаты по этой ферме
    next_payment_at = db.Column(db.DateTime, nullable=True)          # дата следующей оплаты
    next_payment_amount = db.Column(db.Integer, nullable=True)       # сумма в рублях
    next_payment_tariff = db.Column(db.Integer, nullable=True)       # выбранный тариф (фиксируется даже при ручной цене)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Связи
    server = db.relationship(
        "Server",
        back_populates="accounts",
    )

    owner = db.relationship(
        "User",
        back_populates="accounts",
    )

    # Если захочешь логировать ресурсы и действия по аккаунту —
    # эти связи уже готовы и согласованы с AccountResourceSnapshot / ActionLog.
    resource_snapshots = db.relationship(
        "AccountResourceSnapshot",
        back_populates="account",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    action_logs = db.relationship(
        "ActionLog",
        back_populates="account",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        db.UniqueConstraint("owner_id", "name", name="uq_accounts_owner_name"),
    )

    def __repr__(self) -> str:
        return f"<Account id={self.id} name={self.name}>"


class ClientConfigVisibility(db.Model):
    """Правила отображения конфигов для клиентов."""

    __tablename__ = "client_config_visibility"

    id = db.Column(db.Integer, primary_key=True)
    script_id = db.Column(db.String(128), nullable=False)
    config_key = db.Column(db.String(128), nullable=False)
    client_visible = db.Column(db.Boolean, default=True, nullable=False)
    client_label = db.Column(db.Text, nullable=True)
    order_index = db.Column(db.Integer, default=0, nullable=False)
    scope = db.Column(db.String(32), default="global", nullable=False)

    __table_args__ = (
        db.Index("idx_client_config_visibility_script_scope", "script_id", "scope"),
    )

    def __repr__(self) -> str:
        return (
            f"<ClientConfigVisibility script={self.script_id} key={self.config_key} "
            f"scope={self.scope}>"
        )


class GlobalInfoMessage(db.Model):
    """Единое сообщение, отображаемое в блоке «Инфо» у всех клиентов."""

    __tablename__ = "global_info_messages"

    id = db.Column(db.Integer, primary_key=True)
    message_text = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<GlobalInfoMessage id={self.id} updated_at={self.updated_at}>"


class TelegramSubscriber(db.Model):
    """Профиль Telegram для клиента UsersDash."""

    __tablename__ = "telegram_subscribers"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True)
    chat_id = db.Column(db.String(64), nullable=False, unique=True)
    username = db.Column(db.String(64), nullable=True)
    first_name = db.Column(db.String(128), nullable=True)
    last_name = db.Column(db.String(128), nullable=True)
    timezone = db.Column(db.String(64), nullable=True)
    allow_broadcasts = db.Column(db.Boolean, default=True, nullable=False)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_interaction_at = db.Column(db.DateTime, nullable=True)

    owner = db.relationship(
        "User",
        back_populates="telegram_profile",
    )

    def __repr__(self) -> str:
        return f"<TelegramSubscriber user_id={self.user_id} chat_id={self.chat_id}>"


class FarmData(db.Model):
    """
    Дополнительные данные по ферме, которые заполняет сам клиент:
    email / логин / пароль / IGG ID / королевство аккаунта / телеграм-контакт.
    Эти данные используются как источник истины для RssV7 и кабинета клиента.
    """
    __tablename__ = "farm_data"

    id = db.Column(db.Integer, primary_key=True)

    # Привязка к аккаунту (ферме)
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=False)

    # Владелец этих данных (клиент в UsersDash)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    # Имя фермы (должно совпадать с Account.name внутри этого пользователя)
    farm_name = db.Column(db.String(128), nullable=False)

    # Контакты и доступ
    email = db.Column(db.String(255), nullable=True)
    login = db.Column(db.String(255), nullable=True)
    password = db.Column(db.String(255), nullable=True)   # TODO: при необходимости заменить на шифрование
    igg_id = db.Column(db.String(64), nullable=True)

    # Королевство аккаунта (игровой сервер), чтобы не путать с сервером бота
    server = db.Column(db.String(64), nullable=True)

    # Telegram-тег клиента / аккаунта, например "@EtoZheMax"
    telegram_tag = db.Column(db.String(64), nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    # Обратная связь на User
    owner = db.relationship(
        "User",
        backref=db.backref("farm_data_entries", lazy="dynamic"),
    )

    account = db.relationship(
        "Account",
        backref=db.backref("farm_data_entry", uselist=False),
    )

    __table_args__ = (
        db.UniqueConstraint("account_id", name="uq_farm_data_account_id"),
    )

    def __repr__(self) -> str:
        return (
            f"<FarmData id={self.id} account_id={self.account_id} user_id={self.user_id} "
            f"farm_name={self.farm_name}>"
        )


class AccountResourceSnapshot(db.Model):
    __tablename__ = "account_resource_snapshots"

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    food = db.Column(db.BigInteger, default=0)
    wood = db.Column(db.BigInteger, default=0)
    stone = db.Column(db.BigInteger, default=0)
    gold = db.Column(db.BigInteger, default=0)
    gems = db.Column(db.BigInteger, default=0)

    account = db.relationship("Account", back_populates="resource_snapshots")

    def __repr__(self):
        return f"<Snapshot acc={self.account_id} at={self.created_at}>"


class ActionLog(db.Model):
    __tablename__ = "action_logs"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)

    action_type = db.Column(db.String(64), nullable=False)
    payload_json = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    ip_address = db.Column(db.String(64), nullable=True)

    user = db.relationship("User", backref=db.backref("action_logs", lazy="dynamic"))
    account = db.relationship("Account", back_populates="action_logs")

    def __repr__(self):
        return f"<ActionLog {self.action_type} by={self.user_id} acc={self.account_id}>"


class SettingsAuditLog(db.Model):
    __tablename__ = "settings_audit_log"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    actor_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)

    action_type = db.Column(db.String(64), nullable=False)
    field_name = db.Column(db.String(128), nullable=True, index=True)
    old_value = db.Column(db.Text, nullable=True)
    new_value = db.Column(db.Text, nullable=True)
    extra_json = db.Column(db.Text, nullable=True)

    ip_address = db.Column(db.String(64), nullable=True)
    user_agent = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    user = db.relationship("User", foreign_keys=[user_id])
    actor = db.relationship("User", foreign_keys=[actor_id])
    account = db.relationship("Account")

    __table_args__ = (
        db.Index("idx_settings_audit_user_created", "user_id", "created_at"),
    )

    def __repr__(self):
        return (
            f"<SettingsAuditLog action={self.action_type} user={self.user_id} "
            f"actor={self.actor_id}>"
        )

  
