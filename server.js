// backGifts/server.js
import "dotenv/config";
import express from "express";
import mongoose from "mongoose";
import cors from "cors";
import { Telegraf, Markup } from "telegraf";
import crypto from "crypto";

import User from "./models/User.js";
import Category from "./models/Category.js";
import Product from "./models/Product.js";
import Manager from "./models/Manager.js";


import path from "path";
import fs from "fs";

console.log("CWD:", process.cwd());
console.log("ENV FILE EXISTS:", fs.existsSync(path.resolve(process.cwd(), ".env")));
console.log("MONGODB_URI at runtime:", process.env.MONGODB_URI);

let bot = null;

const app = express();

// CORS
app.use(
  cors({
    origin: (origin, cb) => cb(null, true),
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "x-admin-token"],
  })
);
app.use(express.json());

// MongoDB
console.log("MONGODB_URI at runtime:", process.env.MONGODB_URI);
mongoose
  .connect(process.env.MONGODB_URI)
  .then(() => console.log("✅ MongoDB connected"))
  .catch((err) => console.error("❌ MongoDB error:", err));

// ==== helper’ы рефераки ===

function genRefCode() {
  return Math.random().toString(36).slice(2, 8); // 6 симолов
}

async function ensureUserRefCode(user) {
  if (user?.referral?.code) return user.referral.code;
  let code = genRefCode();
  for (let i = 0; i < 5; i++) {
    const exists = await User.findOne({ "referral.code": code }, { _id: 1 }).lean();
    if (!exists) break;
    code = genRefCode();
  }
  await User.updateOne(
    { _id: user._id },
    { $set: { "referral.code": code } }
  );
  return code;
}

async function attachReferralIfAny(newUser, refRaw) {
  const ref = String(refRaw || "").trim();
  if (!ref) return;

  let inviter = await User.findOne({ "referral.code": ref });
  if (!inviter && /^\d+$/.test(ref)) {
    inviter = await User.findOne({ telegramId: ref });
  }
  if (!inviter) return;
  if (String(inviter.telegramId) === String(newUser.telegramId)) return;

  await User.updateOne(
    { _id: newUser._id, "referral.referredBy": { $in: [null, undefined] } },
    {
      $set: {
        "referral.referredBy": inviter?.username
          ? String(inviter.username)
          : String(inviter.telegramId),
        "referral.referredByCode": inviter.referral?.code || null,
        "referral.referredAt": new Date(),
      },
    }
  );

  await User.updateOne(
    { _id: inviter._id },
    {
      $inc: { "referral.referralsCount": 1 },
      $push: {
        "referral.referrals": {
          telegramId: String(newUser.telegramId),
          at: new Date(),
        },
      },
    }
  );
}

function requireAdmin(req, res, next) {
  const token = req.header("x-admin-token") || "";
  if (!process.env.ADMIN_API_TOKEN) {
    return res.status(500).json({ ok: false, error: "ADMIN_API_TOKEN is not set" });
  }
  if (token !== process.env.ADMIN_API_TOKEN) {
    return res.status(401).json({ ok: false, error: "Unauthorized" });
  }
  next();
}

// ==== API ====

app.get("/ping", (_, res) => res.json({ ok: true }));

// регистрируем юзера из mini-app
app.post("/register-user", async (req, res) => {
  try {
    const { telegramId, username, firstName, lastName, photoUrl, ref } = req.body;
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    let user = await User.findOne({ telegramId });

    if (!user) {
      const newUser = await User.create({
        telegramId,
        username: username || null,
        firstName: firstName || null,
        lastName: lastName || null,
        photoUrl: photoUrl || null,
      });

      const code = await ensureUserRefCode(newUser);
      await attachReferralIfAny(newUser, ref);

      const fresh = await User.findById(newUser._id).lean();
      return res.json({ ok: true, user: fresh });
    }

    // update существующего
    user.username = username || user.username;
    user.firstName = firstName || user.firstName;
    user.lastName = lastName || user.lastName;
    user.photoUrl = photoUrl || user.photoUrl;
    await user.save();

    res.json({ ok: true, user });
  } catch (e) {
    console.error("/register-user error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// получить юзера
app.get("/get-user", async (req, res) => {
  try {
    const { telegramId } = req.query;
    if (!telegramId) return res.status(400).json({ ok: false, error: "telegramId is required" });

    const user = await User.findOne({ telegramId });
    if (!user) return res.status(404).json({ ok: false, error: "User not found" });

    res.json({ ok: true, user });
  } catch (e) {
    console.error("/get-user error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Telegram prepared share (rich preview like “via @bot”) =====
app.post("/tg/prepared-referral-message", async (req, res) => {
  try {
    if (!bot) {
      return res.status(500).json({ ok: false, error: "Bot disabled (no TELEGRAM_BOT_TOKEN)" });
    }

    const { tgUserId, refCode } = req.body || {};

    const userId = Number(tgUserId);
    if (!userId || Number.isNaN(userId)) {
      return res.status(400).json({ ok: false, error: "tgUserId is required" });
    }

    const code = String(refCode || "").trim();
    if (!code) {
      return res.status(400).json({ ok: false, error: "refCode is required" });
    }

    // Это ссылка, которую получатель откроет
    const startParam = `ref_${code}`;
    const deepLink = `https://t.me/elfduck_shop_bot?startapp=${encodeURIComponent(startParam)}`;

    // Твой баннер/картинка для карточки
    const photo = "https://blush-impressive-moth-462.mypinata.cloud/ipfs/bafybeihgmrlfe3p5p5dlc2ic7lcobhv6pwl4cp45injv24vuife7dtcowa";

    const caption =
      `🦆 ELF DUCK\n\n` +
      `💸 Залетай по моей ссылке и получи 10% скидки на заказ!`;

    // Уникальный id для inline-result (обязателен)
    const resultId = crypto
      .createHash("sha256")
      .update(`${userId}|${startParam}|${photo}`)
      .digest("hex")
      .slice(0, 32);

    // InlineQueryResultPhoto
    const result = {
      type: "photo",
      id: resultId,
      photo_url: photo,
      thumbnail_url: photo,
      caption,
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [
            {
              text: "Получить бонус",
              url: deepLink,
            },
          ],
        ],
      },
    };

    // Create a PreparedInlineMessage for WebApp.shareMessage()
    const prepared = await bot.telegram.callApi("savePreparedInlineMessage", {
      user_id: userId,
      result,
      // важно: нужно разрешить хотя бы один тип чатов, иначе будет ошибка
      allow_user_chats: true,
      allow_group_chats: true,
      allow_channel_chats: true,
      allow_bot_chats: true,
    });

    return res.json({ ok: true, id: prepared?.id });

} catch (e) {
  console.error("/tg/prepared-referral-message error:", e);
  const tgDesc = e?.response?.description || e?.description || e?.message;
  return res.status(500).json({ ok: false, error: tgDesc || "Server error" });
}
});

// ===== Admin: создать категорию =====

app.get("/categories", async (req, res) => {
  try {
    const onlyActive = String(req.query.active || "1") === "1";
    const filter = onlyActive ? { isActive: true } : {};
    const categories = await Category.find(filter).sort({ sortOrder: 1, createdAt: -1 }).lean();
    res.json({ ok: true, categories });
  } catch (e) {
    console.error("GET /categories error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: получить товары (с фильтром по categoryKey) =====

app.get("/products", async (req, res) => {
  try {
    const onlyActive = String(req.query.active || "1") === "1";
    const filter = onlyActive ? { isActive: true } : {};
    if (req.query.categoryKey) filter.categoryKey = String(req.query.categoryKey);

    const products = await Product.find(filter).lean();

    // totalsByManager (общее количество по товару для каждого менеджера)
    const withTotals = products.map((p) => {
      const map = new Map(); // managerTelegramId -> { totalQty, availableFlavorsCount }

      for (const fl of p.flavors || []) {
        if (fl.isActive === false) continue;

        for (const s of fl.stockByManager || []) {
          const mid = String(s.managerTelegramId || "");
          if (!mid) continue;

          const qty = Number(s.qty || 0);
          const cur = map.get(mid) || { managerTelegramId: mid, totalQty: 0, availableFlavorsCount: 0 };
          cur.totalQty += qty;
          if (qty > 0) cur.availableFlavorsCount += 1;

          map.set(mid, cur);
        }
      }

      return { ...p, totalsByManager: Array.from(map.values()) };
    });

    res.json({ ok: true, products: withTotals });
  } catch (e) {
    console.error("GET /products error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: создать товар =====

app.post("/admin/products", requireAdmin, async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.productKey) return res.status(400).json({ ok: false, error: "productKey is required" });
    if (!b.categoryKey) return res.status(400).json({ ok: false, error: "categoryKey is required" });

    const created = await Product.create({
      productKey: String(b.productKey),
      categoryKey: String(b.categoryKey),
      isActive: b.isActive ?? true,

      title1: b.title1 || "",
      title2: b.title2 || "",
      titleModal: b.titleModal || "",
      price: Number(b.price || 0),

      cardBgUrl: b.cardBgUrl || "",
      cardDuckUrl: b.cardDuckUrl || "",
      orderImgUrl: b.orderImgUrl || "",

      classCardDuck: b.classCardDuck || "",
      classActions: b.classActions || "",
      classNewBadge: b.classNewBadge || "",
      newBadge: b.newBadge || "",

      accentColor: b.accentColor || "",

      flavors: Array.isArray(b.flavors) ? b.flavors : [],
    });

    res.json({ ok: true, product: created });
  } catch (e) {
    console.error("POST /admin/products error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: обновить склад вкуса у менеджера =====

app.patch("/admin/products/:id/flavors/:flavorId/stock", requireAdmin, async (req, res) => {
  try {
    const { id, flavorId } = req.params;
    const { managerTelegramId, qty, updatedByTelegramId } = req.body || {};

    if (!managerTelegramId) {
      return res.status(400).json({ ok: false, error: "managerTelegramId is required" });
    }

    const nextQty = Math.max(0, Number(qty || 0));

    const product = await Product.findById(id);
    if (!product) return res.status(404).json({ ok: false, error: "Product not found" });

    const flavor = product.flavors.id(flavorId);
    if (!flavor) return res.status(404).json({ ok: false, error: "Flavor not found" });

    const mid = String(managerTelegramId);

    const existing = (flavor.stockByManager || []).find((s) => String(s.managerTelegramId) === mid);

    if (existing) {
      existing.qty = nextQty;
      existing.updatedAt = new Date();
      existing.updatedByTelegramId = String(updatedByTelegramId || "");
    } else {
      flavor.stockByManager.push({
        managerTelegramId: mid,
        qty: nextQty,
        updatedAt: new Date(),
        updatedByTelegramId: String(updatedByTelegramId || ""),
      });
    }

    await product.save();
    res.json({ ok: true, product });
  } catch (e) {
    console.error("PATCH stock error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});


// ==== Telegram бот ====

const TG_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const WEBAPP_URL = process.env.WEBAPP_URL || "";
const START_BANNER_URL = process.env.START_BANNER_URL || "";

if (TG_BOT_TOKEN) {
  bot = new Telegraf(TG_BOT_TOKEN);

  bot.start(async (ctx) => {
    try {
      const payload = ctx.startPayload || ""; // ref_XXXX и т.п.
      const tgId = String(ctx.from?.id || "");
      let me = tgId ? await User.findOne({ telegramId: tgId }) : null;

      if (!me && tgId) {
        me = await User.create({
          telegramId: tgId,
          username: ctx.from?.username || null,
          firstName: ctx.from?.first_name || null,
          lastName: ctx.from?.last_name || null,
        });
      }

      let myRefCode = null;
      if (me) {
        myRefCode = await ensureUserRefCode(me);
      }

      let openLink = WEBAPP_URL;
      try {
        const u = new URL(WEBAPP_URL);
        if (payload) u.searchParams.set("startapp", payload);
        if (myRefCode) u.searchParams.set("ref", myRefCode);
        openLink = u.toString();
      } catch {
        const params = new URLSearchParams();
        if (payload) params.set("startapp", payload);
        if (myRefCode) params.set("ref", myRefCode);
        openLink = `${WEBAPP_URL}${params.toString() ? "?" + params.toString() : ""}`;
      }

      const caption = [
        "Добро пожаловать в ELF DUCK SHOP!",
      ].join("\n");

      const keyboard = Markup.inlineKeyboard([
        [Markup.button.webApp("💨 Посетить магазин 🛍️", openLink)],
      ]);

      if (START_BANNER_URL) {
        await ctx.replyWithPhoto({ url: START_BANNER_URL }, { caption, ...keyboard });
      } else {
        await ctx.reply(caption, keyboard);
      }
    } catch (e) {
      console.error("bot.start error:", e);
    }
  });
} else {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN not set — bot disabled");
}

// старт сервера
const PORT = process.env.PORT || 8080;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server running on port ${PORT}`);
  if (bot) {
    bot.launch().then(() => console.log("✅ Bot launched"));
  }
});