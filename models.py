from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import DateTime, Integer, String, Text, func, JSON
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

# --- EXISTING TABLES (WAITLIST & LEAD GEN) ---

class Lead(Base):
    __tablename__ = "leads"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(CITEXT(), unique=True, index=True, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

class EmailEvent(Base):
    __tablename__ = "email_events"
    __table_args__ = (
        sa.UniqueConstraint("email", "event_type", name="email_events_email_event_type_key"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    sent_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=False),
        server_default=func.now(),
        nullable=False,
    )

class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"
    email: Mapped[str] = mapped_column(Text, primary_key=True)
    unsubscribed_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=False),
        server_default=func.now(),
        nullable=False,
    )

class LeadIPEvent(Base):
    __tablename__ = "lead_ip_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ip: Mapped[str] = mapped_column(String, index=True, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

# --- NEW PHASE 2 TABLES (FLORIDA LOTTERY DATA) ---

class DrawPick3(Base):
    __tablename__ = "draws_pick3"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draw_datetime: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True, unique=True)
    digit_1: Mapped[int] = mapped_column(Integer)
    digit_2: Mapped[int] = mapped_column(Integer)
    digit_3: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class DrawPick4(Base):
    __tablename__ = "draws_pick4"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draw_datetime: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True, unique=True)
    digit_1: Mapped[int] = mapped_column(Integer)
    digit_2: Mapped[int] = mapped_column(Integer)
    digit_3: Mapped[int] = mapped_column(Integer)
    digit_4: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class DrawPick5(Base):
    __tablename__ = "draws_pick5"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draw_datetime: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True, unique=True)
    digit_1: Mapped[int] = mapped_column(Integer)
    digit_2: Mapped[int] = mapped_column(Integer)
    digit_3: Mapped[int] = mapped_column(Integer)
    digit_4: Mapped[int] = mapped_column(Integer)
    digit_5: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class DrawFantasy5(Base):
    __tablename__ = "draws_fantasy5"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draw_datetime: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True, unique=True)
    numbers: Mapped[list[int]] = mapped_column(JSON)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class DrawCashPop(Base):
    __tablename__ = "draws_cashpop"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draw_datetime: Mapped[DateTime] = mapped_column(DateTime(timezone=True), index=True, unique=True)
    number: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class ComputedStatistic(Base):
    __tablename__ = "computed_statistics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    game_type: Mapped[str] = mapped_column(String, index=True)
    metric_name: Mapped[str] = mapped_column(String, index=True)
    metric_value: Mapped[dict] = mapped_column(JSON)
    computed_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# --- AUTH ---

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(CITEXT(), unique=True, index=True, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_login_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
