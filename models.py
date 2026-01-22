from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


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

    # DB is TIMESTAMP (no tz) per Alembic’s diff
    sent_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=False),
        server_default=func.now(),
        nullable=False,
    )


class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"

    email: Mapped[str] = mapped_column(Text, primary_key=True)

    # DB is TIMESTAMP (no tz) per Alembic’s diff
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

