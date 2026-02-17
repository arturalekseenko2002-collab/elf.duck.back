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
import PickupPoint from "./models/PickupPoint.js"; 
import Cart from "./models/Cart.js";



let bot = null;

const app = express();

// CORS
const corsOptions = {
  origin: (origin, cb) => cb(null, true),
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "x-admin-token"],
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json());

// MongoDB
mongoose
  .connect(process.env.MONGODB_URI)
  .then(() => console.log("✅ MongoDB connected"))
  .catch((err) => console.error("❌ MongoDB error:", err));

// ==== helper’ы рефераки ===

function genRefCode() {
  return Math.random().toString(36).slice(2, 8); // 6 симолов
}

function translitRuToLat(input) {
  const s = String(input || "").trim().toLowerCase();
  const map = {
    а:"a", б:"b", в:"v", г:"g", д:"d", е:"e", ё:"e", ж:"zh", з:"z", и:"i", й:"y",
    к:"k", л:"l", м:"m", н:"n", о:"o", п:"p", р:"r", с:"s", т:"t", у:"u", ф:"f",
    х:"h", ц:"ts", ч:"ch", ш:"sh", щ:"sch", ъ:"", ы:"y", ь:"", э:"e", ю:"yu", я:"ya",
  };

  let out = "";
  for (const ch of s) {
    if (map[ch] !== undefined) out += map[ch];
    else if (/[a-z0-9]/.test(ch)) out += ch;
    else out += "-";
  }

  out = out.replace(/-+/g, "-").replace(/^-+/, "").replace(/-+$/, "");
  if (out.length < 2) out = "category";
  if (out.length > 32) out = out.slice(0, 32).replace(/-+$/, "");
  return out;
}

async function ensureUniqueCategoryKey(baseKey) {
  let key = String(baseKey || "").trim();
  if (!key) key = "category";

  const exists0 = await Category.findOne({ key }, { _id: 1 }).lean();
  if (!exists0) return key;

  for (let i = 2; i <= 50; i++) {
    const suffix = `-${i}`;
    const cut = Math.max(0, 32 - suffix.length);
    const candidate = `${key.slice(0, cut).replace(/-+$/, "")}${suffix}`;
    const exists = await Category.findOne({ key: candidate }, { _id: 1 }).lean();
    if (!exists) return candidate;
  }

  return `${key.slice(0, 24).replace(/-+$/, "")}-${Date.now().toString(36).slice(-6)}`;
}

async function ensureUniqueProductKey(baseKey) {
  let key = String(baseKey || "").trim();
  if (!key) key = "product";

  const exists0 = await Product.findOne({ productKey: key }, { _id: 1 }).lean();
  if (!exists0) return key;

  for (let i = 2; i <= 50; i++) {
    const suffix = `-${i}`;
    const cut = Math.max(0, 32 - suffix.length);
    const candidate = `${key.slice(0, cut).replace(/-+$/, "")}${suffix}`;
    const exists = await Product.findOne({ productKey: candidate }, { _id: 1 }).lean();
    if (!exists) return candidate;
  }

  return `${key.slice(0, 24).replace(/-+$/, "")}-${Date.now().toString(36).slice(-6)}`;
}

async function ensureUniquePickupPointKey(baseKey) {
  let key = String(baseKey || "").trim();
  if (!key) key = "point";

  const exists0 = await PickupPoint.findOne({ key }, { _id: 1 }).lean();
  if (!exists0) return key;

  for (let i = 2; i <= 50; i++) {
    const suffix = `-${i}`;
    const cut = Math.max(0, 32 - suffix.length);
    const candidate = `${key.slice(0, cut).replace(/-+$/, "")}${suffix}`;
    const exists = await PickupPoint.findOne({ key: candidate }, { _id: 1 }).lean();
    if (!exists) return candidate;
  }

  return `${key.slice(0, 24).replace(/-+$/, "")}-${Date.now().toString(36).slice(-6)}`;
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

// ===== Public: pickup points =====
app.get("/pickup-points", async (req, res) => {
  try {
    const onlyActive = String(req.query.active || "1") === "1";
    const filter = onlyActive ? { isActive: true } : {};
    const points = await PickupPoint.find(filter).sort({ sortOrder: 1, createdAt: -1 }).lean();
    res.json({ ok: true, pickupPoints: points });
  } catch (e) {
    console.error("GET /pickup-points error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: pickup points (CRUD) =====
app.post("/admin/pickup-points", requireAdmin, async (req, res) => {
  try {
    const b = req.body || {};
    const rawTitle = String(b.title || "").trim();
    const rawAddress = String(b.address || "").trim();

    if (!rawTitle && !rawAddress) {
      return res.status(400).json({ ok: false, error: "title or address is required" });
    }

    const baseKey = b.key ? String(b.key) : translitRuToLat(rawTitle || rawAddress);
    const finalKey = await ensureUniquePickupPointKey(baseKey);

    const allowed = Array.isArray(b.allowedAdminTelegramIds)
      ? b.allowedAdminTelegramIds.map((x) => String(x)).filter(Boolean)
      : [];

    const created = await PickupPoint.create({
      key: finalKey,
      title: rawTitle,
      address: rawAddress,
      sortOrder: Number(b.sortOrder || 0),
      isActive: b.isActive ?? true,
      allowedAdminTelegramIds: allowed,
    });

    res.json({ ok: true, pickupPoint: created });
  } catch (e) {
    console.error("POST /admin/pickup-points error:", e);
    if (e?.code === 11000) return res.status(409).json({ ok: false, error: "Pickup point key already exists" });
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.patch("/admin/pickup-points/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const b = req.body || {};

    const allow = ["key", "title", "address", "sortOrder", "isActive", "allowedAdminTelegramIds"];
    const update = {};
    for (const k of allow) if (b[k] !== undefined) update[k] = b[k];

    if (update.key !== undefined) update.key = String(update.key);
    if (update.title !== undefined) update.title = String(update.title);
    if (update.address !== undefined) update.address = String(update.address);
    if (update.sortOrder !== undefined) update.sortOrder = Number(update.sortOrder || 0);
    if (update.isActive !== undefined) update.isActive = !!update.isActive;

    if (update.allowedAdminTelegramIds !== undefined) {
      update.allowedAdminTelegramIds = Array.isArray(update.allowedAdminTelegramIds)
        ? update.allowedAdminTelegramIds.map((x) => String(x)).filter(Boolean)
        : [];
    }

    const updated = await PickupPoint.findByIdAndUpdate(id, update, { new: true });
    if (!updated) return res.status(404).json({ ok: false, error: "Pickup point not found" });

    res.json({ ok: true, pickupPoint: updated });
  } catch (e) {
    console.error("PATCH /admin/pickup-points/:id error:", e);
    if (e?.code === 11000) return res.status(409).json({ ok: false, error: "Pickup point key already exists" });
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.delete("/admin/pickup-points/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await PickupPoint.findByIdAndDelete(id);
    if (!deleted) return res.status(404).json({ ok: false, error: "Pickup point not found" });
    res.json({ ok: true });
  } catch (e) {
    console.error("DELETE /admin/pickup-points/:id error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: categories =====

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

// ===== Admin: создать категорию =====
app.post("/admin/categories", requireAdmin, async (req, res) => {
  try {
    const b = req.body || {};
    if (!b.title) {
      return res.status(400).json({ ok: false, error: "title is required" });
    }

    // auto key if omitted
    const rawTitle = String(b.title || "");
    const baseKey = b.key ? String(b.key) : translitRuToLat(rawTitle);
    const finalKey = await ensureUniqueCategoryKey(baseKey);

    const created = await Category.create({
      key: finalKey,
      title: String(b.title),
      isActive: b.isActive ?? true,

      cardBgUrl: b.cardBgUrl || "",
      cardDuckUrl: b.cardDuckUrl || "",
      classCardDuck: b.classCardDuck || "",
      titleClass: b.titleClass || "cardTitle",
      showOverlay: !!b.showOverlay,
      badgeText: b.badgeText || "",
      badgeSide: (b.badgeSide === "right" ? "right" : "left"),
      sortOrder: Number(b.sortOrder || 0),
    });

    return res.json({ ok: true, category: created });
  } catch (e) {
    console.error("POST /admin/categories error:", e);
    // duplicate key
    if (e?.code === 11000) {
      return res.status(409).json({ ok: false, error: "Category key already exists" });
    }
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: обновить категорию =====
app.patch("/admin/categories/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const b = req.body || {};

    const update = {};
    const allow = [
      "key",
      "title",
      "isActive",
      "cardBgUrl",
      "cardDuckUrl",
      "classCardDuck",
      "titleClass",
      "showOverlay",
      "badgeText",
      "badgeSide",
      "sortOrder",
    ];

    for (const k of allow) {
      if (b[k] !== undefined) update[k] = b[k];
    }

    if (update.key !== undefined) update.key = String(update.key);
    if (update.title !== undefined) update.title = String(update.title);
    if (update.sortOrder !== undefined) update.sortOrder = Number(update.sortOrder || 0);
    if (update.showOverlay !== undefined) update.showOverlay = !!update.showOverlay;
    if (update.badgeSide !== undefined) {
      update.badgeSide = (update.badgeSide === "right" ? "right" : "left");
    }

    const cat = await Category.findByIdAndUpdate(id, update, { new: true });
    if (!cat) return res.status(404).json({ ok: false, error: "Category not found" });

    return res.json({ ok: true, category: cat });
  } catch (e) {
    console.error("PATCH /admin/categories/:id error:", e);
    if (e?.code === 11000) {
      return res.status(409).json({ ok: false, error: "Category key already exists" });
    }
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: получить товары (с фильтром по categoryKey) =====

app.get("/products", async (req, res) => {
  try {
    const onlyActive = String(req.query.active || "1") === "1";
    const filter = onlyActive ? { isActive: true } : {};
    if (req.query.categoryKey) filter.categoryKey = String(req.query.categoryKey);

    const products = await Product.find(filter).sort({ sortOrder: 1, createdAt: -1 }).lean();

    // totalsByManager (общее количество по товару для каждого менеджера)
    const withTotals = products.map((p) => {
      const map = new Map(); // managerTelegramId -> { totalQty, availableFlavorsCount }

      for (const fl of p.flavors || []) {
        if (fl.isActive === false) continue;

        for (const s of fl.stockByPickupPoint || []) {
          const mid = String(s.pickupPointId || "");
          if (!mid) continue;

          const qty = Number(s.totalQty || 0);
          const cur = map.get(mid) || { pickupPointId: mid, totalQty: 0, availableFlavorsCount: 0 };
          cur.totalQty += qty;
          if (qty > 0) cur.availableFlavorsCount += 1;

          map.set(mid, cur);
        }
      }

      return { ...p, totalsByPickupPoint: Array.from(map.values()) };
    });

    res.json({ ok: true, products: withTotals });
  } catch (e) {
    console.error("GET /products error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: get cart by telegramId =====
app.get("/cart", async (req, res) => {
  try {
    const telegramId = String(req.query.telegramId || "").trim();
    if (!telegramId) return res.status(400).json({ ok: false, error: "telegramId is required" });

    const cart = await Cart.findOne({ telegramId }).lean();
    res.json({ ok: true, cart: cart || { telegramId, items: [], checkoutPickupPointId: null } });
  } catch (e) {
    console.error("GET /cart error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: replace cart (save full state) =====
app.put("/cart", async (req, res) => {
  try {
    const b = req.body || {};
    const telegramId = String(b.telegramId || "").trim();
    if (!telegramId) return res.status(400).json({ ok: false, error: "telegramId is required" });

    const items = Array.isArray(b.items) ? b.items : [];
    const checkoutPickupPointId = b.checkoutPickupPointId || null;

    // минимальная нормализация
    const cleanItems = items
      .map((it) => ({
        productKey: String(it.productKey || "").trim(),
        flavorKey: String(it.flavorKey || "").trim(),
        qty: Math.max(1, Number(it.qty || 1)),

        unitPrice: Number(it.unitPrice || 0),

        pickupPointId: it.pickupPointId || null,

        flavorLabel: String(it.flavorLabel || ""),
        gradient: Array.isArray(it.gradient) ? it.gradient.slice(0, 2) : [],
      }))
      .filter((it) => it.productKey && it.flavorKey);

    const updated = await Cart.findOneAndUpdate(
      { telegramId },
      {
        $set: {
          telegramId,
          items: cleanItems,
          checkoutPickupPointId,
        },
      },
      { upsert: true, new: true }
    ).lean();

    res.json({ ok: true, cart: updated });
  } catch (e) {
    console.error("PUT /cart error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: создать товар =====

app.post("/admin/products", requireAdmin, async (req, res) => {
  try {
    
    const b = req.body || {};
    const t1 = String(b.title1 || "").trim();
    const t2 = String(b.title2 || "").trim();
    const baseFromTitle = translitRuToLat([t1, t2].filter(Boolean).join(" "));
    const baseKey = b.productKey ? String(b.productKey) : baseFromTitle;
    const finalProductKey = await ensureUniqueProductKey(baseKey);

    const created = await Product.create({
      productKey: finalProductKey,
      sortOrder: Number(b.sortOrder || 0),
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

// ===== Admin: обновить товар (categoryKey, isActive, media, UI fields) =====
app.patch("/admin/products/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const b = req.body || {};

    const allow = [
      "productKey",
      "sortOrder",
      "categoryKey",
      "isActive",
      "title1",
      "title2",
      "titleModal",
      "price",
      "cardBgUrl",
      "cardDuckUrl",
      "orderImgUrl",
      "classCardDuck",
      "classActions",
      "classNewBadge",
      "newBadge",
      "accentColor",
    ];

    const update = {};
    for (const k of allow) {
      if (b[k] !== undefined) update[k] = b[k];
    }

    if (update.productKey !== undefined) update.productKey = String(update.productKey);
    if (update.sortOrder !== undefined) update.sortOrder = Number(update.sortOrder || 0);
    if (update.categoryKey !== undefined) update.categoryKey = String(update.categoryKey);
    if (update.title1 !== undefined) update.title1 = String(update.title1);
    if (update.title2 !== undefined) update.title2 = String(update.title2);
    if (update.titleModal !== undefined) update.titleModal = String(update.titleModal);
    if (update.price !== undefined) update.price = Number(update.price || 0);
    if (update.cardBgUrl !== undefined) update.cardBgUrl = String(update.cardBgUrl);
    if (update.cardDuckUrl !== undefined) update.cardDuckUrl = String(update.cardDuckUrl);
    if (update.orderImgUrl !== undefined) update.orderImgUrl = String(update.orderImgUrl);
    if (update.classCardDuck !== undefined) update.classCardDuck = String(update.classCardDuck);
    if (update.classActions !== undefined) update.classActions = String(update.classActions);
    if (update.classNewBadge !== undefined) update.classNewBadge = String(update.classNewBadge);
    if (update.newBadge !== undefined) update.newBadge = String(update.newBadge);
    if (update.accentColor !== undefined) update.accentColor = String(update.accentColor);

    const updated = await Product.findByIdAndUpdate(id, update, { new: true });
    if (!updated) return res.status(404).json({ ok: false, error: "Product not found" });

    return res.json({ ok: true, product: updated });
  } catch (e) {
    console.error("PATCH /admin/products/:id error:", e);
    // duplicate key
    if (e?.code === 11000) {
      return res.status(409).json({ ok: false, error: "Product key already exists" });
    }
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: создать/обновить вкус у товара =====
app.post("/admin/products/:id/flavors", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const b = req.body || {};

    const flavorKey = String(b.flavorKey || "").trim().toLowerCase();
    const label = String(b.label || "").trim();

    if (!flavorKey) {
      return res.status(400).json({ ok: false, error: "flavorKey is required" });
    }
    if (!label) {
      return res.status(400).json({ ok: false, error: "label is required" });
    }

    const gradient = Array.isArray(b.gradient) ? b.gradient.map((x) => String(x)) : [];
    if (gradient.length !== 2) {
      return res.status(400).json({ ok: false, error: "gradient must contain exactly 2 colors" });
    }

    const product = await Product.findById(id);
    if (!product) return res.status(404).json({ ok: false, error: "Product not found" });

    const existing = (product.flavors || []).find(
      (f) => String(f.flavorKey || "").toLowerCase() === flavorKey
    );

    if (existing) {
      // update flavor meta
      existing.label = label;
      existing.gradient = gradient;
      if (b.isActive !== undefined) existing.isActive = !!b.isActive;
    } else {
      // create new flavor
      product.flavors.push({
        flavorKey,
        label,
        isActive: b.isActive ?? true,
        gradient,
        stockByPickupPoint: [],
      });
    }

    await product.save();
    return res.json({ ok: true, product });
  } catch (e) {
    console.error("POST /admin/products/:id/flavors error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: обновить склад вкуса по точке самовывоза =====
app.patch("/admin/products/:id/flavors/:flavorId/stock", requireAdmin, async (req, res) => {
  try {
    const { id, flavorId } = req.params;
    const { pickupPointId, totalQty, updatedByTelegramId } = req.body || {};

    if (!pickupPointId) {
      return res.status(400).json({ ok: false, error: "pickupPointId is required" });
    }

    const nextQty = Math.max(0, Number(totalQty ?? 0));

    const product = await Product.findById(id);
    if (!product) return res.status(404).json({ ok: false, error: "Product not found" });

    const flavor = product.flavors.id(flavorId);
    if (!flavor) return res.status(404).json({ ok: false, error: "Flavor not found" });

    const pid = String(pickupPointId);

    const existing = (flavor.stockByPickupPoint || []).find(
      (s) => String(s.pickupPointId) === pid
    );

    if (existing) {
      existing.totalQty = nextQty;
      // reservedQty пока не трогаем (вы сказали резерв позже)
      existing.updatedAt = new Date();
      existing.updatedByTelegramId = String(updatedByTelegramId || "");
    } else {
      flavor.stockByPickupPoint.push({
        pickupPointId, // важно: сюда приходит ObjectId строки, mongoose приведёт
        totalQty: nextQty,
        reservedQty: 0,
        updatedAt: new Date(),
        updatedByTelegramId: String(updatedByTelegramId || ""),
      });
    }

    await product.save();
    return res.json({ ok: true, product });
  } catch (e) {
    console.error("PATCH /admin/products/:id/flavors/:flavorId/stock error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
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