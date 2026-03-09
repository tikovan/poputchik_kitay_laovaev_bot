@router.callback_query(F.data.startswith("popular:"))
async def popular_route_open(callback: CallbackQuery):
    await callback.answer()
    try:
        _, from_country, to_country = callback.data.split(":", 2)
        rows = search_route_posts_all(from_country, to_country, limit=20)

        if not rows:
            await callback.message.answer("По этому маршруту сейчас нет активных объявлений.")
            return

        trips = sum(1 for r in rows if r["post_type"] == TYPE_TRIP)
        parcels = sum(1 for r in rows if r["post_type"] == TYPE_PARCEL)

        await callback.message.answer(
            f"Маршрут: <b>{html.escape(from_country)} → {html.escape(to_country)}</b>\n"
            f"Найдено: <b>{len(rows)}</b>\n"
            f"✈️ Попутчиков: <b>{trips}</b>\n"
            f"📦 Посылок: <b>{parcels}</b>"
        )

        for row in rows:
            try:
                text = post_text(row)
                if len(text) > 4000:
                    text = text[:3900] + "\n\n..."
                await callback.message.answer(
                    text,
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )
            except Exception as inner_e:
                print(f"POPULAR_ROUTE_SEND_ROW ERROR: {inner_e}")

    except Exception as e:
        print(f"POPULAR_ROUTE_OPEN ERROR: {e}")
        await callback.message.answer("Не удалось открыть маршрут.")


@router.message(F.text == "🆕 Новые объявления")
async def recent_posts_handler(message: Message):
    rows = get_recent_posts(10)
    if not rows:
        await message.answer("Пока нет новых активных объявлений.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("🆕 Последние объявления:")
    for row in rows:
        await message.answer(
            f"{post_text(row)}\n\n<b>Добавлено:</b> {format_age(row['created_at'])}",
            reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
        )


@router.message(F.text == "📊 Статистика")
async def stats_handler(message: Message):
    stats = service_stats()
    route = top_route()
    text = (
        "📊 <b>Статистика сервиса</b>\n\n"
        f"Пользователей: <b>{stats['users_count']}</b>\n"
        f"Активных объявлений: <b>{stats['active_posts']}</b>\n"
        f"✈️ Попутчиков: <b>{stats['active_trips']}</b>\n"
        f"📦 Посылок: <b>{stats['active_parcels']}</b>\n"
    )
    if route:
        text += f"\nПопулярный маршрут:\n<b>{route['from_country']} → {route['to_country']}</b> ({route['cnt']})"
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(F.text == "🔔 Подписки")
async def subscriptions_menu(message: Message):
    await message.answer("Подписки на маршруты:", reply_markup=subscription_actions_kb())


@router.callback_query(F.data == "sub:new")
async def sub_new_start(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Хочу получать попутчиков для посылки", callback_data="subtype:trip")],
        [InlineKeyboardButton(text="Хочу получать посылки для маршрута", callback_data="subtype:parcel")]
    ])
    await state.set_state(SubscriptionFlow.looking_for)
    await callback.message.answer("Что отслеживать?", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("subtype:"))
async def sub_type(callback: CallbackQuery, state: FSMContext):
    post_type = callback.data.split(":")[1]
    await state.update_data(post_type=post_type)
    await state.set_state(SubscriptionFlow.from_country)
    await callback.message.answer("Выбери страну отправления:", reply_markup=countries_kb("subfrom"))
    await callback.answer()


@router.callback_query(F.data.startswith("subfrom:"))
async def sub_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(SubscriptionFlow.to_country)
    await callback.message.answer("Выбери страну назначения:", reply_markup=countries_kb("subto"))
    await callback.answer()


@router.callback_query(F.data.startswith("subto:"))
async def sub_to(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    data = await state.get_data()
    add_route_subscription(callback.from_user.id, data["post_type"], data["from_country"], country)
    await state.clear()
    await callback.message.answer(
        f"✅ Подписка сохранена: {data['from_country']} → {country}\n"
        "Бот будет присылать новые подходящие объявления.",
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "sub:list")
async def sub_list(callback: CallbackQuery):
    subs = list_route_subscriptions(callback.from_user.id)
    if not subs:
        await callback.message.answer("У вас пока нет подписок.")
    else:
        rows = []
        for s in subs:
            label = f"{s['id']} • {('✈️' if s['post_type'] == 'trip' else '📦')} • {s['from_country']}→{s['to_country']}"
            rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"subdel:{s['id']}")])
        await callback.message.answer(
            "Ваши подписки. Нажмите на нужную, чтобы удалить:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
    await callback.answer()


@router.callback_query(F.data.startswith("subdel:"))
async def sub_delete(callback: CallbackQuery):
    sub_id = int(callback.data.split(":")[1])
    ok = delete_subscription(callback.from_user.id, sub_id)
    await callback.answer("Подписка удалена" if ok else "Не найдено", show_alert=True)


@router.callback_query(F.data.startswith("contact:"))
async def contact_owner(callback: CallbackQuery, state: FSMContext):
    _, post_id, owner_id = callback.data.split(":")
    if int(owner_id) == callback.from_user.id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    deal_id = ensure_deal(
        post_id=int(post_id),
        owner_user_id=int(owner_id),
        requester_user_id=callback.from_user.id,
        initiator_user_id=callback.from_user.id
    )

    await state.set_state(ContactFlow.message_text)
    await state.update_data(post_id=int(post_id), target_user_id=int(owner_id), deal_id=deal_id)
    await callback.message.answer("Напиши сообщение владельцу объявления. Я перешлю его через бота.")
    await callback.answer()


@router.message(ContactFlow.message_text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data["target_user_id"]
    post_id = data["post_id"]
    deal_id = data.get("deal_id")
    text = (message.text or "").strip()

    if not text:
        await message.answer("Сообщение не должно быть пустым.")
        return

    try:
        from_name = html.escape(message.from_user.full_name or "Пользователь")
        username_part = f" (@{html.escape(message.from_user.username)})" if message.from_user.username else ""
        safe_text = html.escape(text)

        await message.bot.send_message(
            target_user_id,
            f"Новое сообщение по объявлению ID {post_id}:\n\n"
            f"От: {from_name}{username_part}\n\n{safe_text}\n\n"
            "Чтобы ответить, напиши пользователю напрямую или дождись предложения сделки."
        )

        with closing(connect_db()) as conn, conn:
            conn.execute(
                "INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at) VALUES (?, ?, ?, ?)",
                (post_id, target_user_id, message.from_user.id, now_ts())
            )

            if deal_id:
                conn.execute("""
                    UPDATE deals
                    SET status=?, updated_at=?
                    WHERE id=? AND status NOT IN (?, ?, ?)
                """, (DEAL_CONTACTED, now_ts(), deal_id, DEAL_COMPLETED, DEAL_FAILED, DEAL_CANCELLED))

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{target_user_id}")],
            [InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_post:{post_id}")]
        ])

        await message.answer(
            "Сообщение отправлено владельцу объявления.\n\n"
            "Следующий шаг — предложить сделку:",
            reply_markup=kb
        )

    except Exception:
        await message.answer("Не удалось отправить сообщение. Возможно, владелец еще не запускал бота.")

    await state.clear()


@router.callback_query(F.data.startswith("offer_deal:"))
async def offer_deal_handler(callback: CallbackQuery):
    _, post_id_str, owner_id_str = callback.data.split(":")
    post_id = int(post_id_str)
    owner_id = int(owner_id_str)
    requester_id = callback.from_user.id

    if owner_id == requester_id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        await callback.answer("Объявление не найдено или неактивно", show_alert=True)
        return

    deal_id = ensure_deal(
        post_id=post_id,
        owner_user_id=owner_id,
        requester_user_id=requester_id,
        initiator_user_id=requester_id
    )

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_OFFERED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            owner_id,
            f"🤝 Вам предложили сделку по объявлению ID {post_id}.\n\n"
            f"Пользователь: {html.escape(callback.from_user.full_name or 'Пользователь')}"
            + (f" (@{html.escape(callback.from_user.username)})" if callback.from_user.username else ""),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"deal_accept:{deal_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"deal_reject:{deal_id}")
                ]
            ])
        )
    except Exception:
        pass

    await callback.message.answer("Предложение сделки отправлено владельцу.")
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("deal_accept:"))
async def deal_accept_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_ACCEPTED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"✅ Ваша сделка по объявлению ID {deal['post_id']} принята.\n\n"
            "Управление сделками происходит во вкладке МЕНЮ '🤝 Мои сделки'.\n"
            "Там вы сможете закрыть сделку и оставить отзыв.\n\n"
            "Для перехода — откройте МЕНЮ бота."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка принята.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_reject:"))
async def deal_reject_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"❌ Ваша сделка по объявлению ID {deal['post_id']} отклонена."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка отклонена.")
    await callback.answer()


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    upsert_user(message)
    deals = list_user_deals(message.from_user.id)
    if not deals:
        await message.answer("У вас пока нет сделок.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("Ваши сделки:", reply_markup=deal_list_kb(deals))


@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    await callback.answer()
    try:
        deal_id = int(callback.data.split(":")[1])
        deal = get_deal(deal_id)

        if not deal or (deal["owner_user_id"] != callback.from_user.id and deal["requester_user_id"] != callback.from_user.id):
            await callback.message.answer("Сделка не найдена.")
            return

        role = "владелец объявления" if callback.from_user.id == deal["owner_user_id"] else "откликнувшийся"
        other_user_id = deal["requester_user_id"] if callback.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]

        text = (
            f"🤝 <b>Сделка #{deal['id']}</b>\n\n"
            f"Объявление ID: <b>{deal['post_id']}</b>\n"
            f"Статус: <b>{format_deal_status(deal['status'])}</b>\n"
            f"Ваша роль: <b>{role}</b>\n"
            f"Вторая сторона: <b>{format_user_ref(other_user_id)}</b>\n"
            f"Подтверждение владельца: <b>{'Да' if deal['owner_confirmed'] else 'Нет'}</b>\n"
            f"Подтверждение откликнувшегося: <b>{'Да' if deal['requester_confirmed'] else 'Нет'}</b>"
        )

        await callback.message.answer(text, reply_markup=deal_open_kb(deal, callback.from_user.id))

    except Exception as e:
        print(f"OPEN_MY_DEAL ERROR: {e}")
        await callback.message.answer("Не удалось открыть сделку.")


@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] not in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        await callback.answer("Эту сделку нельзя подтвердить", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        if callback.from_user.id == deal["owner_user_id"]:
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        elif callback.from_user.id == deal["requester_user_id"]:
            conn.execute("""
                UPDATE deals
                SET requester_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        else:
            await callback.answer("Нет доступа", show_alert=True)
            return

        refreshed = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()

        if refreshed["owner_confirmed"] and refreshed["requester_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, completed_at=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED, now_ts(), now_ts(), deal_id))
            final_status = DEAL_COMPLETED
        elif refreshed["owner_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_OWNER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_OWNER
        else:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_REQUESTER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_REQUESTER

    other_user_id = (
        deal["requester_user_id"]
        if callback.from_user.id == deal["owner_user_id"]
        else deal["owner_user_id"]
    )

    if final_status == DEAL_COMPLETED:
        for uid in [deal["owner_user_id"], deal["requester_user_id"]]:
            try:
                await callback.bot.send_message(
                    uid,
                    f"✅ Сделка #{deal_id} завершена. Теперь вы можете оставить отзыв.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal_id}")]
                    ])
                )
            except Exception as e:
                print(f"DEAL COMPLETE SEND ERROR: {e}")

        await callback.message.answer("Сделка полностью завершена.")

    else:
        try:
            await callback.bot.send_message(
                other_user_id,
                f"📌 Вторая сторона подтвердила завершение сделки по объявлению ID {deal['post_id']}.\n\n"
                "Чтобы окончательно закрыть сделку, откройте МЕНЮ → '🤝 Мои сделки' "
                "и подтвердите завершение со своей стороны."
            )
        except Exception as e:
            print(f"DEAL CONFIRM NOTIFY ERROR: {e}")

        await callback.message.answer("Ваше подтверждение сохранено. Ждем подтверждение второй стороны.")

    await callback.answer()


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] != DEAL_COMPLETED:
        await callback.answer("Отзыв можно оставить только после завершенной сделки", show_alert=True)
        return

    if callback.from_user.id == deal["owner_user_id"]:
        reviewed_user_id = deal["requester_user_id"]
    elif callback.from_user.id == deal["requester_user_id"]:
        reviewed_user_id = deal["owner_user_id"]
    else:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(reviewed_user_id=reviewed_user_id, post_id=deal["post_id"], deal_id=deal_id)
    await state.set_state(ReviewFlow.rating)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="review_rating:1"),
            InlineKeyboardButton(text="2", callback_data="review_rating:2"),
            InlineKeyboardButton(text="3", callback_data="review_rating:3")
        ],
        [
            InlineKeyboardButton(text="4", callback_data="review_rating:4"),
            InlineKeyboardButton(text="5", callback_data="review_rating:5")
        ]
    ])

    await callback.message.answer("Поставьте оценку пользователю от 1 до 5:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("review_rating:"))
async def review_rating_callback(callback: CallbackQuery, state: FSMContext):
    rating = int(callback.data.split(":")[1])
    await state.update_data(rating=rating)
    await state.set_state(ReviewFlow.text)
    await callback.message.answer("Напишите короткий отзыв или отправьте '-' если без текста.")
    await callback.answer()


@router.message(ReviewFlow.text)
async def review_finish(message: Message, state: FSMContext):
    data = await state.get_data()

    review_text = None if message.text.strip() == "-" else message.text.strip()[:500]
    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    rating = data["rating"]

    with closing(connect_db()) as conn, conn:
        try:
            conn.execute("""
                INSERT INTO reviews (reviewer_user_id, reviewed_user_id, post_id, rating, text, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                rating,
                review_text,
                now_ts()
            ))
        except sqlite3.IntegrityError:
            await message.answer("Такой отзыв уже оставлен.", reply_markup=main_menu(message.from_user.id))
            await state.clear()
            return

    await message.answer("⭐ Спасибо! Отзыв сохранен.", reply_markup=main_menu(message.from_user.id))
    await state.clear()


@router.callback_query(F.data.startswith("deal_fail_post:"))
async def deal_fail_post_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    ok = mark_deal_failed(post_id, callback.from_user.id)
    if ok:
        await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    else:
        await callback.message.answer("Не удалось найти активную сделку для этого объявления.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_fail_direct:"))
async def deal_fail_direct_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or (deal["owner_user_id"] != callback.from_user.id and deal["requester_user_id"] != callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    await callback.answer()


@router.message(F.text == "🆘 Жалоба")
async def complaint_start(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(ComplaintFlow.post_id)
    await message.answer("Введи ID объявления, на которое хочешь пожаловаться.")


@router.callback_query(F.data.startswith("complain:"))
async def complaint_from_button(callback: CallbackQuery, state: FSMContext):
    post_id = int(callback.data.split(":")[1])
    await state.set_state(ComplaintFlow.reason)
    await state.update_data(post_id=post_id)
    await callback.message.answer(f"Опиши причину жалобы на объявление {post_id}.")
    await callback.answer()


@router.message(ComplaintFlow.post_id)
async def complaint_post_id(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Нужен числовой ID объявления.")
        return
    await state.update_data(post_id=int(message.text))
    await state.set_state(ComplaintFlow.reason)
    await message.answer("Опиши причину жалобы.")


@router.message(ComplaintFlow.reason)
async def complaint_reason(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    post_id = data["post_id"]

    row = get_post(post_id)
    if not row:
        await message.answer("Объявление не найдено.", reply_markup=main_menu(message.from_user.id))
        await state.clear()
        return

    owner_user_id = row["user_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, message.text.strip()[:1000], now_ts())
        )

        complaints_count = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM complaints c
            JOIN posts p ON p.id = c.post_id
            WHERE p.user_id = ?
        """, (owner_user_id,)).fetchone()["cnt"]

    if complaints_count >= 3:
        ban_user(owner_user_id)

    await message.answer("Жалоба отправлена.", reply_markup=main_menu(message.from_user.id))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"Новая жалоба на объявление {post_id}:\n\n"
                f"Причина: {html.escape(message.text.strip())}\n\n"
                f"Всего жалоб на пользователя: {complaints_count}\n\n"
                + post_text(row)
            )
        except Exception:
            pass

    if complaints_count >= 3:
        try:
            await bot.send_message(
                owner_user_id,
                "⛔ Ваш аккаунт автоматически заблокирован, потому что на ваши объявления поступило 3 жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception:
            pass

    await state.clear()


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Админка доступна только администраторам.")
        return
    with closing(connect_db()) as conn:
        pending = conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status='pending'").fetchone()["c"]
        complaints = conn.execute("SELECT COUNT(*) AS c FROM complaints").fetchone()["c"]
    await message.answer(
        f"Админка\n\nНа модерации: {pending}\nЖалоб: {complaints}\n\n"
        "Команды:\n"
        "/admin_pending\n"
        "/admin_complaints\n"
        "/admin_ban USER_ID\n"
        "/admin_unban USER_ID\n"
        "/admin_verify USER_ID\n"
        "/admin_unverify USER_ID"
    )


@router.message(Command("admin_pending"))
async def admin_pending(message: Message):
    if not is_admin(message.from_user.id):
        return
    with closing(connect_db()) as conn:
        rows = conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='pending'
            ORDER BY p.created_at ASC
            LIMIT 20
        """).fetchall()
    if not rows:
        await message.answer("Нет объявлений на модерации.")
        return
    for row in rows:
        await message.answer(post_text(row), reply_markup=admin_post_actions_kb(row["id"]))


@router.callback_query(F.data.startswith("adminapprove:"))
async def admin_approve(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    post_id = int(callback.data.split(":")[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE posts SET status='active', updated_at=? WHERE id=?", (now_ts(), post_id))
    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} одобрено.")
    if row:
        try:
            await bot.send_message(row["user_id"], f"Ваше объявление {post_id} одобрено и опубликовано.")
        except Exception:
            pass
    await safe_publish(bot, post_id)
    await notify_coincidence_users(bot, post_id)
    await notify_subscribers(bot, post_id)
    await callback.answer("Одобрено")


@router.callback_query(F.data.startswith("adminreject:"))
async def admin_reject(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    post_id = int(callback.data.split(":")[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE posts SET status='rejected', updated_at=? WHERE id=?", (now_ts(), post_id))
    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} отклонено.")
    if row:
        try:
            await bot.send_message(row["user_id"], f"Ваше объявление {post_id} отклонено модератором.")
        except Exception:
            pass
    await callback.answer("Отклонено")


@router.callback_query(F.data.startswith("adminbanpost:"))
async def admin_ban_post_owner(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return

    ban_user(row["user_id"])

    try:
        await bot.send_message(row["user_id"], "Ваш аккаунт был ограничен администратором.")
    except Exception:
        pass
    await callback.message.answer(f"Пользователь {row['user_id']} забанен, его объявления деактивированы.")
    await callback.answer("Готово")


@router.message(Command("admin_complaints"))
async def admin_complaints(message: Message):
    if not is_admin(message.from_user.id):
        return
    with closing(connect_db()) as conn:
        rows = conn.execute("""
            SELECT c.*, p.user_id as post_owner_id, p.post_type, p.from_country, p.to_country
            FROM complaints c
            LEFT JOIN posts p ON p.id=c.post_id
            ORDER BY c.created_at DESC
            LIMIT 20
        """).fetchall()
    if not rows:
        await message.answer("Жалоб нет.")
        return
    for row in rows:
        await message.answer(
            f"Жалоба #{row['id']}\n"
            f"Объявление: {row['post_id']}\n"
            f"От пользователя: {row['from_user_id']}\n"
            f"Владелец объявления: {row['post_owner_id']}\n"
            f"Маршрут: {row['from_country']} → {row['to_country']}\n"
            f"Причина: {html.escape(row['reason'])}"
        )


@router.message(Command("admin_ban"))
async def admin_ban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_ban USER_ID")
        return
    user_id = int(parts[1])
    ban_user(user_id)
    await message.answer(f"Пользователь {user_id} забанен.")


@router.message(Command("admin_unban"))
async def admin_unban(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_unban USER_ID")
        return
    user_id = int(parts[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (user_id,))
    await message.answer(f"Пользователь {user_id} разбанен.")


@router.message(Command("admin_verify"))
async def admin_verify(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_verify USER_ID")
        return
    user_id = int(parts[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user_id,))
    await message.answer(f"Пользователь {user_id} отмечен как проверенный.")


@router.message(Command("admin_unverify"))
async def admin_unverify(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_unverify USER_ID")
        return
    user_id = int(parts[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_verified=0 WHERE user_id=?", (user_id,))
    await message.answer(f"Статус проверенного у пользователя {user_id} снят.")


@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    text = (
        "<b>Помощь</b>\n\n"
        "✈️ <b>Взять посылку</b> — если вы летите и можете что-то передать.\n"
        "📦 <b>Отправить посылку</b> — если вам нужно что-то передать.\n"
        "🔎 <b>Найти совпадения</b> — быстрый поиск подходящих объявлений.\n"
        "📋 <b>Мои объявления</b> — управление своими объявлениями.\n"
        "🤝 <b>Мои сделки</b> — ваши активные и завершенные сделки.\n"
        "🔔 <b>Подписки</b> — уведомления по нужным маршрутам.\n"
        "🆘 <b>Жалоба</b> — пожаловаться на объявление."
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


# -------------------------
# RUN
# -------------------------

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    init_db()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    asyncio.create_task(expire_old_posts(bot))
    asyncio.create_task(global_coincidence_loop(bot))

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
