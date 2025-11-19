# auth.py
# Маршруты авторизации: логин, логаут.

from datetime import datetime

from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, current_user
from werkzeug.security import check_password_hash

from models import db, User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """
    Форма логина.
    GET: показать форму.
    POST: проверить логин/пароль и залогинить пользователя.
    """
    if current_user.is_authenticated:
        # Уже залогинен — отправляем на главную
        return redirect(url_for("index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        # Защита от пустых полей
        if not username or not password:
            flash("Введите логин и пароль.", "danger")
            return render_template("auth/login.html")

        user = User.query.filter_by(username=username).first()

        # Проверяем существование пользователя и пароль
        if not user or not check_password_hash(user.password_hash, password):
            flash("Неверный логин или пароль.", "danger")
            return render_template("auth/login.html")

        if not user.is_active:
            flash("Учетная запись заблокирована. Обратитесь к администратору.", "danger")
            return render_template("auth/login.html")

        # Всё ок — логиним
        login_user(user)

        # Обновляем время последнего входа
        user.last_login_at = datetime.utcnow()
        db.session.commit()

        flash("Вы успешно вошли в систему.", "success")
        return redirect(url_for("index"))

    return render_template("auth/login.html")


@auth_bp.route("/logout")
def logout():
    """
    Выход пользователя из системы.
    """
    if current_user.is_authenticated:
        logout_user()
        flash("Вы вышли из системы.", "success")
    return redirect(url_for("auth.login"))
