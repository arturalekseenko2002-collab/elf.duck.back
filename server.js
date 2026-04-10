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
import Order from "./models/Order.js";

const APP_URL = String(process.env.APP_URL || process.env.WEBAPP_URL || "https://elf-duck.vercel.app").trim();

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

function ensureReferralGroupsArray(user) {
  if (!user.referral) user.referral = {};
  user.referral.rewardGroups = Array.isArray(user.referral.rewardGroups)
    ? user.referral.rewardGroups
    : [];
  return user.referral.rewardGroups;
}

function attachReferralToRewardGroup(ownerUser, referredTelegramId) {
  const referralId = String(referredTelegramId || "").trim();
  if (!ownerUser || !referralId) return false;

  const groups = ensureReferralGroupsArray(ownerUser);

  const alreadyAdded = groups.some((group) =>
    Array.isArray(group?.memberTelegramIds) &&
    group.memberTelegramIds.map((x) => String(x)).includes(referralId)
  );
  if (alreadyAdded) return false;

  let targetGroup = groups.find(
    (group) =>
      group?.rewardClaimed !== true &&
      Array.isArray(group?.memberTelegramIds) &&
      group.memberTelegramIds.length < 2
  );

  if (!targetGroup) {
    groups.push({
      pairIndex: groups.length + 1,
      memberTelegramIds: [referralId],
      rewardClaimed: false,
      rewardClaimedAt: null,
      rewardAmountZl: 25,
    });

    if (typeof ownerUser.markModified === "function") {
      ownerUser.markModified("referral.rewardGroups");
    }

    return true;
  }

  const currentIds = Array.isArray(targetGroup.memberTelegramIds)
    ? targetGroup.memberTelegramIds.map((x) => String(x)).filter(Boolean)
    : [];

  if (!currentIds.includes(referralId)) {
    currentIds.push(referralId);
  }

  targetGroup.memberTelegramIds = currentIds;

  if (typeof ownerUser.markModified === "function") {
    ownerUser.markModified("referral.rewardGroups");
  }

  return true;
}

function getReferralDisplayName(user) {
  if (!user) return "Пользователь";
  if (user.username) return `@${String(user.username).trim()}`;
  if (user.firstName) return String(user.firstName).trim();
  return String(user.telegramId || "Пользователь");
}

async function markReferralFirstOrderDoneIfNeeded(telegramId) {
  const safeTelegramId = String(telegramId || "").trim();
  if (!safeTelegramId) return false;

  const referredUser = await User.findOne({ telegramId: safeTelegramId });
  if (!referredUser) return false;

  if (referredUser?.referral?.firstOrderDoneAt) return false;

  const inviterCode = String(referredUser?.referral?.usedCode || "").trim();
  if (!inviterCode) return false;

  referredUser.referral = referredUser.referral || {};
  referredUser.referral.firstOrderDoneAt = new Date();
  await referredUser.save();

  return true;
}

async function buildReferralStatusForUser(ownerUser) {
  if (!ownerUser) {
    return {
      code: "",
      totalReferrals: 0,
      referralsCount: 0,
      availableClaims: 0,
      groups: [],
    };
  }

  const groups = ensureReferralGroupsArray(ownerUser);

  const memberIds = groups.flatMap((group) =>
    Array.isArray(group?.memberTelegramIds)
      ? group.memberTelegramIds.map((x) => String(x)).filter(Boolean)
      : []
  );

  const referredUsers = memberIds.length
    ? await User.find(
        { telegramId: { $in: memberIds } },
        { telegramId: 1, username: 1, firstName: 1, photoUrl: 1, createdAt: 1, referral: 1 }
      ).lean()
    : [];

  const referredById = new Map(
    referredUsers.map((row) => [String(row.telegramId || ""), row])
  );

  const paidOrderTelegramIds = memberIds.length
    ? await Order.distinct("userTelegramId", {
        userTelegramId: { $in: memberIds },
        $or: [
          { "payment.status": "paid" },
          { status: { $in: ["processing", "done"] } },
        ],
      })
    : [];

  const paidSet = new Set((paidOrderTelegramIds || []).map((x) => String(x || "")));

  const mappedGroups = groups.map((group) => {
    const members = (Array.isArray(group?.memberTelegramIds) ? group.memberTelegramIds : []).map((tgId) => {
      const safeTgId = String(tgId || "");
      const refUser = referredById.get(safeTgId);
      const hasConfirmedFirstPurchase =
        Boolean(refUser?.referral?.firstOrderDoneAt) || paidSet.has(safeTgId);

      return {
        telegramId: safeTgId,
        invitedAt: refUser?.createdAt || null,
        username: String(refUser?.username || ""),
        firstName: String(refUser?.firstName || ""),
        photoUrl: String(refUser?.photoUrl || ""),
        displayName: getReferralDisplayName(refUser || { telegramId: safeTgId }),
        firstOrderDoneAt: refUser?.referral?.firstOrderDoneAt || null,
        completed: hasConfirmedFirstPurchase,
      };
    });

    const completedCount = members.filter((m) => m.completed === true).length;
    const isComplete = members.length === 2;
    const isClaimed = group?.rewardClaimed === true;
    const readyToClaim = isComplete && completedCount === 2 && !isClaimed;

    return {
      id: String(group?._id || ""),
      pairIndex: Number(group?.pairIndex || 0),
      rewardAmountZl: Number(group?.rewardAmountZl || 25),
      rewardZl: Number(group?.rewardAmountZl || 25),
      rewardClaimed: isClaimed,
      rewardClaimedAt: group?.rewardClaimedAt || null,
      claimedAt: group?.rewardClaimedAt || null,
      completedCount,
      isComplete,
      isClaimed,
      isClaimable: readyToClaim,
      readyToClaim,
      members,
    };
  });

  return {
    code: String(ownerUser?.referral?.code || ""),
    totalReferrals: referredUsers.length,
    referralsCount: referredUsers.length,
    availableClaims: mappedGroups.filter((g) => g.isClaimable).length, //d
    groups: mappedGroups,
  };
}

function genOrderNo() {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let out = "ED-";
  for (let i = 0; i < 6; i++) out += alphabet[Math.floor(Math.random() * alphabet.length)];
  return out;
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatOrderDate(dt) {
  try {
    return new Date(dt).toLocaleString("ru-RU", {
      timeZone: "Europe/Warsaw",
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return String(dt || "—");
  }
}

function buildOrderPointSearchBlob(order, pickupPoint) {
  const rawParts = [
    pickupPoint?.key,
    pickupPoint?.title,
    pickupPoint?.address,
    pickupPoint?.name,
    pickupPoint?.label,
    pickupPoint?.district,
    pickupPoint?.city,
    order?.pickupPointTitle,
    order?.pickupPointAddress,
    order?.methodLabel,
    order?.deliveryMethod,
    order?.deliveryType,
  ];

  return rawParts
    .map((v) => String(v || "").trim().toLowerCase())
    .filter(Boolean)
    .join(" | ");
}

function normalizePhotoLookupText(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ł/g, "l")
    .replace(/ś/g, "s")
    .replace(/ż/g, "z")
    .replace(/ź/g, "z")
    .replace(/ć/g, "c")
    .replace(/ń/g, "n")
    .replace(/ó/g, "o")
    .replace(/ą/g, "a")
    .replace(/ę/g, "e")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstNonEmptyString(...values) {
  for (const value of values) {
    const str = String(value || "").trim();
    if (str) return str;
  }
  return "";
}

function getManagerOrderPhotoByPickupPoint(order, pickupPoint) {
  const deliveryType = normalizePhotoLookupText(order?.deliveryType);
  const deliveryMethod = normalizePhotoLookupText(order?.deliveryMethod);

  if (deliveryType === "delivery" && deliveryMethod.includes("courier")) {
    return firstNonEmptyString(
      process.env.TG_ORDER_PHOTO_COURIER,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (deliveryType === "delivery" && deliveryMethod.includes("inpost")) {
    return firstNonEmptyString(
      process.env.TG_ORDER_PHOTO_INPOST,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  const pointKey = normalizePhotoLookupText(buildOrderPointSearchBlob(order, pickupPoint));

  if (pointKey.includes("praga")) {
    return firstNonEmptyString(
      process.env.TG_ORDER_PHOTO_PRAGA,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (pointKey.includes("mokotow")) {
    return firstNonEmptyString(
      process.env.TG_ORDER_PHOTO_MOKOTOW,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (pointKey.includes("wola")) {
    return firstNonEmptyString(
      process.env.TG_ORDER_PHOTO_WOLA,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (
    pointKey.includes("srodmiescie") ||
    pointKey.includes("sródmiescie") ||
    pointKey.includes("śródmiescie")
  ) {
    return firstNonEmptyString(
      process.env.TG_ORDER_PHOTO_SRODMIESCIE,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  return firstNonEmptyString(process.env.TG_ORDER_PHOTO_DEFAULT);
}

function getCustomerOrderPhotoByPickupPoint(order, pickupPoint) {
  const deliveryType = normalizePhotoLookupText(order?.deliveryType);
  const deliveryMethod = normalizePhotoLookupText(order?.deliveryMethod);

  if (deliveryType === "delivery" && deliveryMethod.includes("courier")) {
    return firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_COURIER,
      process.env.TG_ORDER_PHOTO_COURIER,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (deliveryType === "delivery" && deliveryMethod.includes("inpost")) {
    return firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_INPOST,
      process.env.TG_ORDER_PHOTO_INPOST,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  const pointKey = normalizePhotoLookupText(buildOrderPointSearchBlob(order, pickupPoint));

  if (pointKey.includes("praga")) {
    return firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_PRAGA,
      process.env.TG_ORDER_PHOTO_PRAGA,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (pointKey.includes("mokotow")) {
    return firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_MOKOTOW,
      process.env.TG_ORDER_PHOTO_MOKOTOW,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (pointKey.includes("wola")) {
    return firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_WOLA,
      process.env.TG_ORDER_PHOTO_WOLA,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  if (
    pointKey.includes("srodmiescie") ||
    pointKey.includes("sródmiescie") ||
    pointKey.includes("śródmiescie")
  ) {
    return firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_SRODMIESCIE,
      process.env.TG_ORDER_PHOTO_SRODMIESCIE,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
  }

  return firstNonEmptyString(
    process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
    process.env.TG_ORDER_PHOTO_DEFAULT
  );
}

const WARSAW_DELIVERY_DISTRICT_PRICES = new Map([
  ["srodmiescie", { label: "Śródmieście", price: 20 }],
  ["wola", { label: "Wola", price: 20 }],
  ["ochota", { label: "Ochota", price: 20 }],
  ["zoliborz", { label: "Żoliborz", price: 20 }],
  ["praga-polnoc", { label: "Praga Północ", price: 20 }],

  ["mokotow", { label: "Mokotów", price: 20 }],
  ["praga-poludnie", { label: "Praga Południe", price: 20 }],
  ["bialoleka", { label: "Białołęka", price: 20 }],
  ["targowek", { label: "Targówek", price: 20 }],
  ["bielany", { label: "Bielany", price: 20 }],
  ["bemowo", { label: "Bemowo", price: 20 }],
  ["ursus", { label: "Ursus", price: 20 }],
  ["wlochy", { label: "Włochy", price: 20 }],
  ["ursynow", { label: "Ursynów", price: 20 }],
  ["wilanow", { label: "Wilanów", price: 20 }],

  ["wawer", { label: "Wawer", price: 25 }],
  ["rembertow", { label: "Rembertów", price: 25 }],
  ["wesola", { label: "Wesoła", price: 25 }],
]);

function normalizeDistrictChunk(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ł/g, "l")
    .replace(/ś/g, "s")
    .replace(/ż/g, "z")
    .replace(/ź/g, "z")
    .replace(/ć/g, "c")
    .replace(/ń/g, "n")
    .replace(/ó/g, "o")
    .replace(/ą/g, "a")
    .replace(/ę/g, "e")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function getProductCategoryKey(product) {
  return String(product?.categoryKey || "").trim().toLowerCase();
}

function getInpostEquivalentUnitsFromCartItems(items = [], products = []) {
  const productByKey = new Map(
    (Array.isArray(products) ? products : []).map((p) => [String(p?.productKey || "").trim(), p])
  );

  const totals = (Array.isArray(items) ? items : []).reduce(
    (acc, item) => {
      const qty = Math.max(0, Number(item?.qty || 0));
      const product = productByKey.get(String(item?.productKey || "").trim());
      const categoryKey = getProductCategoryKey(product);

      if (categoryKey === "liquids") {
        acc.liquids += qty;
      } else if (categoryKey === "disposables" || categoryKey === "pods") {
        acc.devices += qty;
      } else if (categoryKey === "cartridges") {
        acc.cartridges += qty;
      }

      return acc;
    },
    { liquids: 0, devices: 0, cartridges: 0 }
  );

  const liquidsUnits = totals.liquids * (7 / 20); // 20 жиж = 7 единиц
  const devicesUnits = totals.devices;            // 1 курилка / 1 под = 1 единица
  const cartridgesUnits = totals.cartridges / 4;  // 4 картриджа = 1 единица

  const packageUnits = Number((liquidsUnits + devicesUnits + cartridgesUnits).toFixed(4));
  return packageUnits;
}

function resolveInpostDeliveryPricing(items = [], products = []) {
  const packageUnits = getInpostEquivalentUnitsFromCartItems(items, products);
  const deliveryFeeZl = packageUnits > 7 ? 17 : 12;

  return {
    packageUnits,
    deliveryFeeZl,
  };
}

async function resolveWarsawDeliveryPricing(address) {
  const rawAddress = String(address || "").trim();
  if (!rawAddress) {
    return {
      districtKey: "",
      districtLabel: null,
      deliveryFeeZl: 0,
      matched: false,
    };
  }

  const normalizeLooseText = (input) =>
    String(input || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/ł/g, "l")
      .replace(/ś/g, "s")
      .replace(/ż/g, "z")
      .replace(/ź/g, "z")
      .replace(/ć/g, "c")
      .replace(/ń/g, "n")
      .replace(/ó/g, "o")
      .replace(/ą/g, "a")
      .replace(/ę/g, "e")
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  const matchDistrictFromText = (input) => {
    const normalizedAddress = normalizeDistrictChunk(input);

    for (const [key, meta] of WARSAW_DELIVERY_DISTRICT_PRICES.entries()) {
      if (normalizedAddress.includes(key)) {
        return {
          districtKey: key,
          districtLabel: meta.label,
          deliveryFeeZl: Number(meta.price || 0),
          matched: true,
        };
      }
    }

    return {
      districtKey: "",
      districtLabel: null,
      deliveryFeeZl: 0,
      matched: false,
    };
  };

  const normalizedRaw = normalizeLooseText(rawAddress);
  const inputTokens = normalizedRaw
    .split(" ")
    .filter(
      (token) =>
        token.length >= 3 &&
        token !== "warszawa" &&
        token !== "warsaw" &&
        token !== "poland" &&
        token !== "polska"
    );

  const inputNumberTokens = normalizedRaw.match(/\b\d+[a-z]?\b/g) || [];

  const looksLikeDistrictOnly =
    inputTokens.length <= 2 && inputNumberTokens.length === 0;

  if (looksLikeDistrictOnly) {
    const directMatch = matchDistrictFromText(rawAddress);
    if (directMatch.matched) {
      return directMatch;
    }
  }

  try {
    const query = encodeURIComponent(`${rawAddress}, Warszawa, Poland`);
    const url = `https://nominatim.openstreetmap.org/search?format=jsonv2&addressdetails=1&limit=5&q=${query}`;

    const response = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "ELF-DUCK/1.0 (delivery district lookup)",
      },
    });

    const data = await response.json().catch(() => []);
    const rows = Array.isArray(data) ? data : [];

    for (const row of rows) {
      const addr = row?.address || {};

      const cityBlob = normalizeLooseText(
        [
          addr.city,
          addr.town,
          addr.village,
          addr.municipality,
          addr.state,
          row?.display_name,
        ]
          .filter(Boolean)
          .join(" ")
      );

      if (!cityBlob.includes("warszawa") && !cityBlob.includes("warsaw")) {
        continue;
      }

      const locationBlob = normalizeLooseText(
        [
          addr.road,
          addr.pedestrian,
          addr.footway,
          addr.path,
          addr.cycleway,
          addr.house_number,
          addr.house,
          addr.building,
          row?.display_name,
        ]
          .filter(Boolean)
          .join(" ")
      );

      const houseNumber = normalizeLooseText(addr.house_number || "");

      const hasMeaningfulAddress = Boolean(
        (addr.road || addr.pedestrian || addr.footway || addr.path || addr.cycleway) &&
          (addr.house_number || addr.house || addr.building)
      );

      if (!hasMeaningfulAddress) {
        continue;
      }

      const matchedWordTokens = inputTokens.filter((token) =>
        locationBlob.includes(token)
      );

      const requiredMatches = inputTokens.length <= 1 ? 1 : Math.min(2, inputTokens.length);

      if (matchedWordTokens.length < requiredMatches) {
        continue;
      }

      if (inputNumberTokens.length > 0) {
        const numberMatched = inputNumberTokens.some(
          (token) =>
            houseNumber === token ||
            locationBlob.includes(` ${token} `) ||
            locationBlob.endsWith(` ${token}`)
        );

        if (!numberMatched) {
          continue;
        }
      }

      const districtCandidates = [
        addr.city_district,
        addr.suburb,
        addr.borough,
        addr.quarter,
        addr.neighbourhood,
      ].filter(Boolean);

      for (const candidate of districtCandidates) {
        const matched = matchDistrictFromText(candidate);
        if (matched.matched) {
          return matched;
        }
      }
    }
  } catch (e) {
    console.error("resolveWarsawDeliveryPricing geocode error:", e);
  }

  return {
    districtKey: "",
    districtLabel: null,
    deliveryFeeZl: 0,
    matched: false,
  };
}

function getWarsawDateKey() {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

function getWarsawNowMinutes() {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Warsaw",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date());

  const hh = Number(parts.find((p) => p.type === "hour")?.value || 0);
  const mm = Number(parts.find((p) => p.type === "minute")?.value || 0);

  return hh * 60 + mm;
}

function timeToMinutes(hhmm) {
  const [h, m] = String(hhmm || "0:0").split(":").map(Number);
  return h * 60 + m;
}

function getTodayScheduleForPickupPoint(point) {
  const dateKey = getWarsawDateKey();
  return point?.scheduleByDate?.[dateKey] || null;
}

function getPointOpenStateNow(point) {
  const schedule = getTodayScheduleForPickupPoint(point);

  if (!schedule) {
    return {
      isOpen: false,
      reason: "NO_SCHEDULE",
      openFrom: "",
      openTo: "",
    };
  }

  if (schedule?.isOpen === false || schedule?.closed === true || schedule?.isActive === false) {
    return {
      isOpen: false,
      reason: "CLOSED_TODAY",
      openFrom: "",
      openTo: "",
    };
  }

  const normalizePeriod = (raw) => {
    if (!raw || typeof raw !== "object") return null;

    const from = String(
      raw?.openFrom ?? raw?.from ?? raw?.start ?? raw?.startTime ?? raw?.timeFrom ?? ""
    ).trim();

    const to = String(
      raw?.openTo ?? raw?.to ?? raw?.end ?? raw?.endTime ?? raw?.timeTo ?? ""
    ).trim();

    if (!from || !to) return null;

    return { openFrom: from, openTo: to };
  };

  const periodsRaw =
    (Array.isArray(schedule?.periods) && schedule.periods) ||
    (Array.isArray(schedule?.timePeriods) && schedule.timePeriods) ||
    (Array.isArray(schedule?.ranges) && schedule.ranges) ||
    (Array.isArray(schedule?.slots) && schedule.slots) ||
    [];

  const normalizedPeriods = periodsRaw
    .map(normalizePeriod)
    .filter(Boolean)
    .sort((a, b) => timeToMinutes(a.openFrom) - timeToMinutes(b.openFrom));

  const fallbackOpenFrom = String(schedule?.openFrom || schedule?.from || "").trim();
  const fallbackOpenTo = String(schedule?.openTo || schedule?.to || "").trim();

  if (!normalizedPeriods.length && fallbackOpenFrom && fallbackOpenTo) {
    normalizedPeriods.push({
      openFrom: fallbackOpenFrom,
      openTo: fallbackOpenTo,
    });
  }

  if (!normalizedPeriods.length) {
    return {
      isOpen: false,
      reason: "NO_HOURS",
      openFrom: "",
      openTo: "",
    };
  }

  const nowMinutes = getWarsawNowMinutes();

  const activePeriod = normalizedPeriods.find((period) => {
    const fromMinutes = timeToMinutes(period.openFrom);
    const toMinutes = timeToMinutes(period.openTo);
    return nowMinutes >= fromMinutes && nowMinutes <= toMinutes;
  });

  if (activePeriod) {
    return {
      isOpen: true,
      reason: "OPEN",
      openFrom: activePeriod.openFrom,
      openTo: activePeriod.openTo,
      periods: normalizedPeriods,
    };
  }

  return {
    isOpen: false,
    reason: "OUTSIDE_HOURS",
    openFrom: normalizedPeriods[0]?.openFrom || "",
    openTo: normalizedPeriods[normalizedPeriods.length - 1]?.openTo || "",
    periods: normalizedPeriods,
  };
}

function isTimeWindowInsidePointSchedule(point, timeWindow) {
  const schedule = getTodayScheduleForPickupPoint(point);
  const rawWindow = String(timeWindow || "").trim();

  if (!schedule || !rawWindow) return false;
  if (schedule?.isOpen === false || schedule?.closed === true || schedule?.isActive === false) {
    return false;
  }

  const match = rawWindow.match(/^(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})$/);
  if (!match) return false;

  const windowFrom = timeToMinutes(match[1]);
  const windowTo = timeToMinutes(match[2]);
  if (windowTo <= windowFrom) return false;

  const periodsRaw =
    (Array.isArray(schedule?.periods) && schedule.periods) ||
    (Array.isArray(schedule?.timePeriods) && schedule.timePeriods) ||
    (Array.isArray(schedule?.ranges) && schedule.ranges) ||
    (Array.isArray(schedule?.slots) && schedule.slots) ||
    [];

  const normalizedPeriods = periodsRaw
    .map((raw) => {
      const from = String(
        raw?.openFrom ?? raw?.from ?? raw?.start ?? raw?.startTime ?? raw?.timeFrom ?? ""
      ).trim();

      const to = String(
        raw?.openTo ?? raw?.to ?? raw?.end ?? raw?.endTime ?? raw?.timeTo ?? ""
      ).trim();

      if (!from || !to) return null;
      return { openFrom: from, openTo: to };
    })
    .filter(Boolean)
    .sort((a, b) => timeToMinutes(a.openFrom) - timeToMinutes(b.openFrom));

  const fallbackOpenFrom = String(schedule?.openFrom || schedule?.from || "").trim();
  const fallbackOpenTo = String(schedule?.openTo || schedule?.to || "").trim();

  if (!normalizedPeriods.length && fallbackOpenFrom && fallbackOpenTo) {
    normalizedPeriods.push({
      openFrom: fallbackOpenFrom,
      openTo: fallbackOpenTo,
    });
  }

  if (!normalizedPeriods.length) return false;

  return normalizedPeriods.some((period) => {
    const periodFrom = timeToMinutes(period.openFrom);
    const periodTo = timeToMinutes(period.openTo);

    return windowFrom >= periodFrom && windowTo <= periodTo;
  });
}

const LIQUIDS_CATEGORY_KEYS = new Set(["liquids"]);
const DISPOSABLES_CATEGORY_KEYS = new Set(["disposables"]);
const CARTRIDGES_CATEGORY_KEYS = new Set(["cartridges"]);

const DISPOSABLES_NO_SMART_PRICE_PRODUCT_KEYS = new Set([
  "elf-duck-1500",
  "elf-duck-1500-2",
]);

function getSmartDiscountPerItem(unitsQty) {
  const qty = Math.max(0, Number(unitsQty || 0));
  if (qty >= 5) return 15;
  if (qty >= 3) return 10;
  if (qty >= 2) return 5;
  return 0;
}

function getCartridgeSmartUnitPrice(unitsQty) {
  const qty = Math.max(0, Number(unitsQty || 0));
  if (qty >= 5) return 20;
  if (qty >= 3) return 23;
  if (qty >= 2) return 25;
  return 30;
}

function isLiquidSmartPriceProduct(product) {
  const categoryKey = String(product?.categoryKey || "").trim().toLowerCase();
  return LIQUIDS_CATEGORY_KEYS.has(categoryKey);
}

function isDisposableSmartPriceProduct(product) {
  const categoryKey = String(product?.categoryKey || "").trim().toLowerCase();
  const productKey = String(product?.productKey || "").trim().toLowerCase();

  return (
    DISPOSABLES_CATEGORY_KEYS.has(categoryKey) &&
    !DISPOSABLES_NO_SMART_PRICE_PRODUCT_KEYS.has(productKey)
  );
}

function isCartridgeSmartPriceProduct(product) {
  const categoryKey = String(product?.categoryKey || "").trim().toLowerCase();
  return CARTRIDGES_CATEGORY_KEYS.has(categoryKey);
}

function repriceCartItemsWithSmartPricing(items, products) {
  const prodByKey = new Map(
    (products || []).map((p) => [String(p?.productKey || "").trim(), p])
  );

  const liquidUnitsQty = (items || []).reduce((sum, it) => {
    const product = prodByKey.get(String(it?.productKey || "").trim());
    if (!isLiquidSmartPriceProduct(product)) return sum;
    return sum + Math.max(1, Number(it?.qty || 1));
  }, 0);

  const disposableUnitsQty = (items || []).reduce((sum, it) => {
    const product = prodByKey.get(String(it?.productKey || "").trim());
    if (!isDisposableSmartPriceProduct(product)) return sum;
    return sum + Math.max(1, Number(it?.qty || 1));
  }, 0);

  const cartridgeUnitsQty = (items || []).reduce((sum, it) => {
    const product = prodByKey.get(String(it?.productKey || "").trim());
    if (!isCartridgeSmartPriceProduct(product)) return sum;
    return sum + Math.max(1, Number(it?.qty || 1));
  }, 0);

  const liquidDiscountPerItem = getSmartDiscountPerItem(liquidUnitsQty);
  const disposableDiscountPerItem = getSmartDiscountPerItem(disposableUnitsQty);
  const cartridgeUnitPrice = getCartridgeSmartUnitPrice(cartridgeUnitsQty);

  const repricedItems = (items || []).map((it) => {
    const product = prodByKey.get(String(it?.productKey || "").trim());
    const fallbackBasePrice = Number(product?.price || it?.unitPrice || 0);

    if (isLiquidSmartPriceProduct(product)) {
      return {
        ...it,
        baseUnitPrice: Number(fallbackBasePrice.toFixed(2)),
        unitPrice: Number(
          Math.max(0, fallbackBasePrice - liquidDiscountPerItem).toFixed(2)
        ),
      };
    }

    if (isDisposableSmartPriceProduct(product)) {
      return {
        ...it,
        baseUnitPrice: Number(fallbackBasePrice.toFixed(2)),
        unitPrice: Number(
          Math.max(0, fallbackBasePrice - disposableDiscountPerItem).toFixed(2)
        ),
      };
    }

    if (isCartridgeSmartPriceProduct(product)) {
      return {
        ...it,
        baseUnitPrice: Number(fallbackBasePrice.toFixed(2)),
        unitPrice: Number(cartridgeUnitPrice.toFixed(2)),
      };
    }

    return {
      ...it,
      baseUnitPrice: Number(fallbackBasePrice.toFixed(2)),
      unitPrice: Number(fallbackBasePrice.toFixed(2)),
    };
  });

  return {
    repricedItems,
    smartPricingMeta: {
      liquidUnitsQty,
      liquidDiscountPerItem,
      disposableUnitsQty,
      disposableDiscountPerItem,
      cartridgeUnitsQty,
      cartridgeUnitPrice,
    },
  };
}

function getCashbackPercentByTotal(totalZl) {
  const total = Number(totalZl || 0);

  if (total >= 501) return 10;
  if (total >= 301) return 9;
  if (total >= 101) return 7;
  return 4;
}

async function getIsReferralFirstOrderDiscountEligible(telegramId, cartItems = []) {
  const safeTelegramId = String(telegramId || "").trim();
  if (!safeTelegramId) {
    return {
      eligible: false,
      applied: false,
      usedCode: "",
      percent: 0,
      totalBeforeDiscount: 0,
      reason: "NO_TELEGRAM_ID",
    };
  }

  const totalBeforeDiscount = Number(
    (Array.isArray(cartItems) ? cartItems : []).reduce((sum, it) => {
      const qty = Math.max(1, Number(it?.qty || 1));
      const baseUnitPrice = Number(it?.baseUnitPrice || it?.unitPrice || 0);
      return sum + qty * baseUnitPrice;
    }, 0).toFixed(2)
  );

  const user = await User.findOne(
    { telegramId: safeTelegramId },
    { telegramId: 1, referral: 1 }
  ).lean();

  const usedCode = String(user?.referral?.usedCode || "").trim();
  if (!usedCode) {
    return {
      eligible: false,
      applied: false,
      usedCode,
      percent: 0,
      totalBeforeDiscount,
      reason: "NO_USED_REFERRAL_CODE",
    };
  }

  if (user?.referral?.firstOrderDoneAt) {
    return {
      eligible: false,
      applied: false,
      usedCode,
      percent: 0,
      totalBeforeDiscount,
      reason: "FIRST_ORDER_ALREADY_DONE",
    };
  }

  const hasPaidOrders = await Order.exists({
    userTelegramId: safeTelegramId,
    $or: [
      { "payment.status": "paid" },
      { status: { $in: ["processing", "done", "completed"] } },
    ],
  });

  if (hasPaidOrders) {
    return {
      eligible: false,
      applied: false,
      usedCode,
      percent: 0,
      totalBeforeDiscount,
      reason: "PAID_ORDER_ALREADY_EXISTS",
    };
  }

  if (totalBeforeDiscount < 65) {
    return {
      eligible: false,
      applied: false,
      usedCode,
      percent: 0,
      totalBeforeDiscount,
      reason: "TOTAL_BELOW_65",
    };
  }

  return {
    eligible: true,
    applied: true,
    usedCode,
    percent: 10,
    totalBeforeDiscount,
    reason: "OK",
  };
}

function applyReferralFirstOrderDiscountToCartItems(items = [], percent = 0) {
  const safePercent = Math.max(0, Number(percent || 0));
  if (!safePercent) {
    return {
      items: (Array.isArray(items) ? items : []).map((it) => ({
        ...it,
        referralFirstOrderDiscountPercent: 0,
        referralFirstOrderDiscountPerItem: 0,
        referralFirstOrderDiscountTotalZl: 0,
      })),
      meta: {
        applied: false,
        percent: 0,
        totalBeforeDiscount: Number(
          (Array.isArray(items) ? items : []).reduce((sum, it) => {
            const qty = Math.max(1, Number(it?.qty || 1));
            const unitPrice = Number(it?.unitPrice || 0);
            return sum + qty * unitPrice;
          }, 0).toFixed(2)
        ),
        totalDiscountZl: 0,
      },
    };
  }

  const factor = (100 - safePercent) / 100;

  const nextItems = (Array.isArray(items) ? items : []).map((it) => {
    const oldUnitPrice = Number(it?.unitPrice || 0);
    const newUnitPrice = Number((oldUnitPrice * factor).toFixed(2));
    const qty = Math.max(1, Number(it?.qty || 1));

  return {
    ...it,
    baseUnitPrice: Number(it?.baseUnitPrice || oldUnitPrice || 0),
    unitPrice: newUnitPrice,
    referralFirstOrderDiscountPercent: safePercent,
    referralFirstOrderDiscountPerItem: Number((oldUnitPrice - newUnitPrice).toFixed(2)),
    referralFirstOrderDiscountTotalZl: Number(((oldUnitPrice - newUnitPrice) * qty).toFixed(2)),
  };
  });

  const totalBeforeDiscount = Number(
    (Array.isArray(items) ? items : []).reduce((sum, it) => {
      const qty = Math.max(1, Number(it?.qty || 1));
      const unitPrice = Number(it?.unitPrice || 0);
      return sum + qty * unitPrice;
    }, 0).toFixed(2)
  );

  const totalAfterDiscount = Number(
    nextItems.reduce((sum, it) => {
      const qty = Math.max(1, Number(it?.qty || 1));
      const unitPrice = Number(it?.unitPrice || 0);
      return sum + qty * unitPrice;
    }, 0).toFixed(2)
  );

  return {
    items: nextItems,
    meta: {
      applied: true,
      percent: safePercent,
      totalBeforeDiscount,
      totalAfterDiscount,
      totalDiscountZl: Number((totalBeforeDiscount - totalAfterDiscount).toFixed(2)),
    },
  };
}

function getOrderReferralFirstOrderDiscountPercent(order) {
  const fromPayment = Number(order?.payment?.referralFirstOrderDiscountPercent || 0);
  if (fromPayment > 0) return fromPayment;

  const fromItem = (Array.isArray(order?.items) ? order.items : []).find(
    (item) => Number(item?.referralFirstOrderDiscountPercent || 0) > 0
  );

  return Number(fromItem?.referralFirstOrderDiscountPercent || 0);
}

function getOrderReferralFirstOrderDiscountTotalZl(order) {
  const fromPayment = Number(order?.payment?.referralFirstOrderDiscountTotalZl || 0);
  if (fromPayment > 0) return Number(fromPayment.toFixed(2));

  const fromItems = Number(
    (Array.isArray(order?.items) ? order.items : []).reduce((sum, item) => {
      return sum + Number(item?.referralFirstOrderDiscountTotalZl || 0);
    }, 0).toFixed(2)
  );
  if (fromItems > 0) return fromItems;

  const referralPercent = getOrderReferralFirstOrderDiscountPercent(order);
  if (referralPercent > 0) {
    const itemsTotalBeforeDiscount = Number(
      (Array.isArray(order?.items) ? order.items : []).reduce((sum, item) => {
        const qty = Math.max(1, Number(item?.qty || 1));
        const unitPrice = Number(item?.unitPrice || 0);
        const itemDiscountPerItem = Number(item?.referralFirstOrderDiscountPerItem || 0);
        return sum + qty * (unitPrice + itemDiscountPerItem);
      }, 0).toFixed(2)
    );

    const discountedItemsTotal = Number(
      (Array.isArray(order?.items) ? order.items : []).reduce((sum, item) => {
        const qty = Math.max(1, Number(item?.qty || 1));
        const unitPrice = Number(item?.unitPrice || 0);
        return sum + qty * unitPrice;
      }, 0).toFixed(2)
    );

    const diffFromItems = Number((itemsTotalBeforeDiscount - discountedItemsTotal).toFixed(2));
    if (diffFromItems > 0) return diffFromItems;

    const subtotalBeforeDiscount = Number(
      order?.payment?.itemsSubtotalBeforeReferralDiscountZl ||
      order?.payment?.subtotalBeforeReferralDiscountZl ||
      order?.payment?.subtotalBeforeDiscountZl ||
      order?.pricing?.itemsSubtotalBeforeReferralDiscountZl ||
      order?.pricing?.subtotalBeforeReferralDiscountZl ||
      order?.pricing?.subtotalBeforeDiscountZl ||
      0
    );

    if (subtotalBeforeDiscount > 0) {
      return Number(((subtotalBeforeDiscount * referralPercent) / 100).toFixed(2));
    }

    const totalBeforeDiscount = Number(
      order?.payment?.totalBeforeReferralDiscountZl ||
      order?.payment?.totalBeforeDiscountZl ||
      order?.pricing?.totalBeforeReferralDiscountZl ||
      order?.pricing?.totalBeforeDiscountZl ||
      0
    );

    const totalAfterDiscount = Number(
      order?.payment?.totalAmount ||
      order?.payment?.amount ||
      order?.totalAmount ||
      order?.amount ||
      0
    );

    if (totalBeforeDiscount > 0 && totalAfterDiscount > 0 && totalBeforeDiscount > totalAfterDiscount) {
      return Number((totalBeforeDiscount - totalAfterDiscount).toFixed(2));
    }
  }

  return 0;
}

function hasOrderReferralFirstOrderDiscount(order) {
  if (Number(getOrderReferralFirstOrderDiscountTotalZl(order) || 0) > 0) return true;
  if (Number(getOrderReferralFirstOrderDiscountPercent(order) || 0) > 0) return true;

  if (order?.payment?.referralFirstOrderDiscountApplied === true) return true;
  if (order?.pricing?.referralFirstOrderDiscountApplied === true) return true;

  const usedCode = String(
    order?.referral?.usedCode ||
    order?.payment?.referralUsedCode ||
    order?.payment?.usedReferralCode ||
    order?.usedReferralCode ||
    ""
  ).trim();

  const subtotalBeforeReferralDiscount = Number(
    order?.payment?.itemsSubtotalBeforeReferralDiscountZl ||
    order?.payment?.subtotalBeforeReferralDiscountZl ||
    order?.payment?.subtotalBeforeDiscountZl ||
    order?.pricing?.itemsSubtotalBeforeReferralDiscountZl ||
    order?.pricing?.subtotalBeforeReferralDiscountZl ||
    order?.pricing?.subtotalBeforeDiscountZl ||
    0
  );

  const totalAmount = Number(
    order?.payment?.totalAmount ||
    order?.payment?.amount ||
    order?.totalAmount ||
    order?.amount ||
    0
  );

  if (usedCode && (subtotalBeforeReferralDiscount >= 65 || totalAmount >= 65)) {
    return true;
  }

  if (String(order?.payment?.smartDiscountType || "").trim() === "referral_first_order") return true;
  if (String(order?.payment?.discountType || "").trim() === "referral_first_order") return true;
  if (String(order?.pricing?.discountType || "").trim() === "referral_first_order") return true;

  return (Array.isArray(order?.items) ? order.items : []).some((item) => {
    return (
      Number(item?.referralFirstOrderDiscountTotalZl || 0) > 0 ||
      Number(item?.referralFirstOrderDiscountPercent || 0) > 0 ||
      Number(item?.referralFirstOrderDiscountPerItem || 0) > 0
    );
  });
}

function addDays(dateLike, days) {
  const dt = new Date(dateLike || Date.now());
  dt.setDate(dt.getDate() + Number(days || 0));
  return dt;
}

function daysUntilDate(dateLike) {
  const now = new Date();
  const target = new Date(dateLike);
  const ms = target.getTime() - now.getTime();
  return Math.ceil(ms / (24 * 60 * 60 * 1000));
}

function recalcUserCashbackBalanceFromLedger(user) {
  const ledger = Array.isArray(user?.cashbackLedger) ? user.cashbackLedger : [];
  const total = ledger.reduce((sum, row) => {
    if (row?.expiredAt) return sum;
    return sum + Math.max(0, Number(row?.remainingZl || 0));
  }, 0);

  user.cashbackBalance = Number(total.toFixed(2));
  return user.cashbackBalance;
}

async function grantManualCashbackToUser(user, amountZl, meta = {}) {
  if (!user) throw new Error("USER_NOT_FOUND");

  const safeAmount = Number(amountZl || 0);
  if (!(safeAmount > 0)) {
    throw new Error("INVALID_CASHBACK_AMOUNT");
  }

  user.cashbackLedger = Array.isArray(user.cashbackLedger) ? user.cashbackLedger : [];

  const now = new Date();
  const expiresAt = addDays(now, 30);

  user.cashbackLedger.push({
    source: "manual_admin_grant",
    amountZl: Number(safeAmount.toFixed(2)),
    remainingZl: Number(safeAmount.toFixed(2)),
    earnedAt: now,
    expiresAt,
    orderId: null,
    // note: String(meta?.note || "").trim(),
    grantedByTelegramId: String(meta?.grantedByTelegramId || "").trim(),
    grantedByUsername: String(meta?.grantedByUsername || "").trim(),
    warnedAt: null,
    expiredAt: null,
    expiredAmountZl: 0,
  });

  recalcUserCashbackBalanceFromLedger(user);
  await user.save();

  return {
    cashbackBalance: Number(user.cashbackBalance || 0),
    grantedAmountZl: Number(safeAmount.toFixed(2)),
    expiresAt,
  };
}

async function sendCashbackExpiringSoonNotification(user, expiringRows) {
  try {
    if (!bot || !user?.telegramId || !Array.isArray(expiringRows) || !expiringRows.length) return;

    const sorted = [...expiringRows].sort(
      (a, b) => new Date(a.expiresAt).getTime() - new Date(b.expiresAt).getTime()
    );

    const first = sorted[0];
    const firstAmount = Number(first?.remainingZl || 0).toFixed(2);
    const firstDays = Math.max(0, daysUntilDate(first?.expiresAt));
    const firstExpireText = formatCashbackExpireDate(first?.expiresAt);

    const otherActiveRows = (Array.isArray(user?.cashbackLedger) ? user.cashbackLedger : [])
      .filter((row) => !row?.expiredAt && Number(row?.remainingZl || 0) > 0)
      .filter((row) => String(row?._id || "") !== String(first?._id || ""))
      .sort((a, b) => new Date(a.expiresAt).getTime() - new Date(b.expiresAt).getTime());

    const remainingAfterFirst = Number(
      otherActiveRows.reduce((sum, row) => sum + Number(row?.remainingZl || 0), 0).toFixed(2)
    );

    const nextRowsText = otherActiveRows.length
      ? `\n\nОстаток после сгорания этой части: <b>${remainingAfterFirst.toFixed(2)} zł</b>\n\nДругие части кэшбека:\n${otherActiveRows
          .map((row) => {
            const expireText = formatCashbackExpireDate(row?.expiresAt);
            return `• ${Number(row?.remainingZl || 0).toFixed(2)} zł — ${expireText}`;
          })
          .join("\n")}`
      : `\n\nПосле сгорания этой части активного кэшбека не останется.`;

    const text = [
      `💰 <b>КЭШБЕК СКОРО СГОРИТ</b>`,
      ``,
      `Твой кэшбек <b>${firstAmount} zł</b> сгорит:`,
      `<b>${firstExpireText}</b>`,
      `(через <b>${firstDays}</b> дн.)`,
      ``,
      `Успей использовать!`,
      nextRowsText,
    ].join("\n");

    await bot.telegram.sendMessage(String(user.telegramId), text, {
      parse_mode: "HTML",
      disable_web_page_preview: true,
    });
  } catch (e) {
    console.error("sendCashbackExpiringSoonNotification error:", e);
  }
}

function formatCashbackExpireDate(date) {
  const d = new Date(date);

  const day = String(d.getDate()).padStart(2, "0");
  const month = String(d.getMonth() + 1).padStart(2, "0");
  const year = d.getFullYear();

  const hours = String(d.getHours()).padStart(2, "0");
  const minutes = String(d.getMinutes()).padStart(2, "0");

  return `${day}.${month}.${year} в ${hours}:${minutes}`;
}

async function sendCashbackExpiredNotification(user, expiredRows) {
  try {
    if (!bot || !user?.telegramId || !Array.isArray(expiredRows) || !expiredRows.length) return;

    const sortedExpired = [...expiredRows].sort(
      (a, b) => new Date(a.expiresAt).getTime() - new Date(b.expiresAt).getTime()
    );

    const expiredSum = Number(
      sortedExpired.reduce((sum, row) => sum + Number(row?.expiredAmountZl || 0), 0).toFixed(2)
    );

    const activeRows = (Array.isArray(user?.cashbackLedger) ? user.cashbackLedger : [])
      .filter((row) => !row?.expiredAt && Number(row?.remainingZl || 0) > 0)
      .sort((a, b) => new Date(a.expiresAt).getTime() - new Date(b.expiresAt).getTime());

    const activeBalance = Number(
      activeRows.reduce((sum, row) => sum + Number(row?.remainingZl || 0), 0).toFixed(2)
    );

    const expiredRowsText = sortedExpired
      .map((row) => {
        const expireText = formatCashbackExpireDate(row?.expiresAt);
        return `• ${Number(row?.expiredAmountZl || 0).toFixed(2)} zł — сгорело ${expireText}`;
      })
      .join("\n");

    const activeRowsText = activeRows.length
      ? `\n\nОставшиеся части кэшбека:\n${activeRows
          .map((row) => {
            const expireText = formatCashbackExpireDate(row?.expiresAt);
            return `• ${Number(row?.remainingZl || 0).toFixed(2)} zł — ${expireText}`;
          })
          .join("\n")}`
      : `\n\nАктивного кэшбека больше не осталось.`;

    const text = [
      `🔥 <b>ЧАСТЬ КЭШБЕКА СГОРЕЛА</b>`,
      ``,
      `Сгорело: <b>${expiredSum.toFixed(2)} zł</b>`,
      expiredRowsText,
      ``,
      `Текущий активный остаток: <b>${activeBalance.toFixed(2)} zł</b>`,
      activeRowsText,
    ].join("\n");

    await bot.telegram.sendMessage(String(user.telegramId), text, {
      parse_mode: "HTML",
      disable_web_page_preview: true,
    });
  } catch (e) {
    console.error("sendCashbackExpiredNotification error:", e);
  }
}

async function processCashbackLedgerExpirations() {
  try {
    const now = new Date();

    const users = await User.find({
      cashbackLedger: { $exists: true, $ne: [] },
    });

    for (const user of users) {
      let changed = false;
      const ledger = Array.isArray(user.cashbackLedger) ? user.cashbackLedger : [];
      const rowsToWarn = [];
      const rowsJustExpired = [];

      for (const row of ledger) {
        if (!row || row.expiredAt) continue;

        const remaining = Math.max(0, Number(row.remainingZl || 0));
        if (remaining <= 0) continue;

        const expiresAt = row.expiresAt ? new Date(row.expiresAt) : null;
        if (!expiresAt) continue;

        if (expiresAt.getTime() <= now.getTime()) {
          const expiredAmount = Math.max(0, Number(row.remainingZl || 0));

          row.expiredAt = now;
          row.remainingZl = 0;
          changed = true;

          if (expiredAmount > 0) {
            rowsJustExpired.push({
              _id: row._id,
              expiredAmountZl: expiredAmount,
              expiresAt: expiresAt,
            });
          }

          continue;
        }

        const daysLeft = daysUntilDate(expiresAt);
        if ((daysLeft === 3 || daysLeft === 4 || daysLeft === 5) && !row.warnedAt) {
          rowsToWarn.push(row);
          row.warnedAt = now;
          changed = true;
        }
      }

      recalcUserCashbackBalanceFromLedger(user);

      if (changed) {
        await user.save();
      }

      if (rowsToWarn.length) {
        await sendCashbackExpiringSoonNotification(user, rowsToWarn);
      }

      if (rowsJustExpired.length) {
        await sendCashbackExpiredNotification(user, rowsJustExpired);
      }
    }
  } catch (e) {
    console.error("processCashbackLedgerExpirations error:", e);
  }
}

async function applyOrderCashback(order) {
  if (!order) return { applied: false, cashbackZl: 0, percent: 0 };

  const orderId = String(order._id || "").trim();
  if (!orderId) return { applied: false, cashbackZl: 0, percent: 0 };

  const freshOrder = await Order.findById(orderId, {
    _id: 1,
    totalZl: 1,
    cashbackAppliedAt: 1,
    cashbackZl: 1,
    userTelegramId: 1,
  });

  if (!freshOrder) return { applied: false, cashbackZl: 0, percent: 0 };
  await markReferralFirstOrderDoneIfNeeded(freshOrder.userTelegramId);

  // защита от повторного начисления
  if (freshOrder.cashbackAppliedAt) {
    return {
      applied: false,
      cashbackZl: Number(freshOrder.cashbackZl || 0),
      percent: getCashbackPercentByTotal(freshOrder.totalZl),
    };
  }

  const percent = getCashbackPercentByTotal(freshOrder.totalZl);
  const cashbackZl = Number(((Number(freshOrder.totalZl || 0) * percent) / 100).toFixed(2));

  const user = await User.findOne({ telegramId: String(freshOrder.userTelegramId || "") });
  if (!user) return { applied: false, cashbackZl: 0, percent };

  user.cashbackLedger = Array.isArray(user.cashbackLedger) ? user.cashbackLedger : [];

  user.cashbackLedger.push({
    sourceOrderId: freshOrder._id,
    amountZl: cashbackZl,
    remainingZl: cashbackZl,
    earnedAt: new Date(),
    expiresAt: addDays(new Date(), 40),
    warnedAt: null,
    expiredAt: null,
  });

  recalcUserCashbackBalanceFromLedger(user);

  await user.save();

  await Order.updateOne(
    { _id: freshOrder._id, cashbackAppliedAt: null },
    {
      $set: {
        cashbackPercent: percent,
        cashbackZl,
        cashbackAppliedAt: new Date(),
      },
    }
  );

  return { applied: true, cashbackZl, percent };
}

async function refundOrderCashback(order) {
  if (!order) return { refunded: false, amount: 0 };

  const orderId = String(order._id || "").trim();
  if (!orderId) return { refunded: false, amount: 0 };

  const freshOrder = await Order.findById(orderId, {
    _id: 1,
    totalZl: 1,
    userTelegramId: 1,
    payment: 1,
  });

  if (!freshOrder) return { refunded: false, amount: 0 };

  const payment = freshOrder.payment?.toObject
    ? freshOrder.payment.toObject()
    : (freshOrder.payment || {});

  const cashbackAppliedZl = Number(payment?.cashbackAppliedZl || 0);

  if (cashbackAppliedZl <= 0) {
    return { refunded: false, amount: 0 };
  }

  // защита от двойного возврата
  if (payment?.cashbackRefundedAt) {
    return { refunded: false, amount: cashbackAppliedZl };
  }

  const user = await User.findOne({
    telegramId: String(freshOrder.userTelegramId || ""),
  });

  if (!user) return { refunded: false, amount: 0 };

  user.cashbackLedger = Array.isArray(user.cashbackLedger) ? user.cashbackLedger : [];

  user.cashbackLedger.push({
    sourceOrderId: freshOrder._id,
    amountZl: cashbackAppliedZl,
    remainingZl: cashbackAppliedZl,
    earnedAt: new Date(),
    expiresAt: addDays(new Date(), 40),
    warnedAt: null,
    expiredAt: null,
  });

  recalcUserCashbackBalanceFromLedger(user);

  await user.save();

  freshOrder.payment = {
    ...payment,
    cashbackRefundedAt: new Date(),
    cashbackAppliedZl: 0,
    cashbackRemainingToPayZl: Number(freshOrder.totalZl || 0),
    cashbackFullyPaid: false,
    cashbackAppliedAt: null,
    method: payment?.method === "cashback" ? null : payment?.method || null,
  };

  await freshOrder.save();

  return { refunded: true, amount: cashbackAppliedZl };
}

async function resolveOrderNotificationPoint(order) {
  if (!order) return null;

  if (order.deliveryType === "pickup" && order.pickupPointId) {
    return await PickupPoint.findById(order.pickupPointId).lean();
  }

  if (order.deliveryType === "delivery") {
    const deliveryKey = order.deliveryMethod === "inpost" ? "delivery-2" : "delivery";
    return await PickupPoint.findOne({
      key: { $in: [deliveryKey, `${deliveryKey},`] }
    }).lean();
  }

  return null;
}

async function notifyManagerClientArrived(order) {
  try {
    if (!bot || !order) return { ok: false, reason: "NO_BOT_OR_ORDER" };

    const point = await resolveOrderNotificationPoint(order);
    const chatId = String(
      order?.payment?.managerMessageChatId || point?.notificationChatId || ""
    ).trim();

    const replyToMessageId = Number(order?.payment?.managerMessageId || 0);

    if (!chatId || !replyToMessageId) {
      return { ok: false, reason: "NO_MANAGER_MESSAGE" };
    }

    const user = await User.findOne(
      { telegramId: String(order.userTelegramId || "") },
      { telegramId: 1, username: 1, firstName: 1 }
    ).lean();

    const customerName =
      (user?.username ? `@${user.username}` : "") ||
      String(user?.firstName || "").trim() ||
      "—";

    const text = [
      `📍 <b>КЛИЕНТ ПРИБЫЛ НА ТОЧКУ САМОВЫВОЗА</b>`,
      ``,
      `🔢 <b>Номер заказа:</b> #${escapeHtml(order.orderNo)}`,
      `👤 <b>Клиент:</b> ${escapeHtml(customerName)}`,
    ].join("\n");

    const sent = await bot.telegram.sendMessage(chatId, text, {
      parse_mode: "HTML",
      reply_to_message_id: replyToMessageId,
      allow_sending_without_reply: true,
      reply_markup: {
        inline_keyboard: [
          [{ text: "✅ Заказ выполнен", callback_data: `mgr_order_completed:${order._id}` }],
        ],
      },
    });

    await Order.updateOne(
      { _id: order._id },
      {
        $push: {
          managerArrivalMessageIds: String(sent?.message_id || ""),
        },
      }
    );

    return { ok: true };
  } catch (e) {
    console.error("notifyManagerClientArrived error:", e);
    return { ok: false, reason: "SEND_ERROR" };
  }
}

async function annulOrderBecauseNoPaymentConfirm(order, options = {}) {
  try {
    if (!order) return { ok: false, reason: "NO_ORDER" };

    const reason = String(options?.reason || "NO_PAYMENT_CONFIRM").trim() || "NO_PAYMENT_CONFIRM";

    const freshOrder = await Order.findById(String(order._id || ""));
    if (!freshOrder) return { ok: false, reason: "NOT_FOUND" };

    const currentStatus = String(freshOrder.status || "").toLowerCase();
    const paymentStatus = String(freshOrder?.payment?.status || "").toLowerCase();

    if (["completed", "done", "shipped", "canceled", "annulled"].includes(currentStatus)) {
      return { ok: false, reason: "ALREADY_FINAL" };
    }

    if (paymentStatus === "paid" || paymentStatus === "checking") {
      return { ok: false, reason: "PAYMENT_ALREADY_IN_PROGRESS" };
    }

    if (!freshOrder.stockCommittedAt && !freshOrder.stockReleasedAt) {
      try {
        await releaseReservedStockForOrder(freshOrder);
        freshOrder.stockReleasedAt = new Date();
      } catch (releaseErr) {
        console.error("annulOrderBecauseNoPaymentConfirm releaseReservedStockForOrder error:", releaseErr);
      }
    }

    freshOrder.status = "annulled";
    freshOrder.annulledAt = new Date();
    freshOrder.annulledReason = reason;
    await freshOrder.save();

    await refreshManagerOrderMessage(freshOrder);

    try {
      if (bot && freshOrder?.userTelegramId) {
        const orderNo = escapeHtml(freshOrder?.orderNo || "—");

        await bot.telegram.sendMessage(
          String(freshOrder.userTelegramId),
          [
            `⌛️ <b>ЗАКАЗ АННУЛИРОВАН</b>`,
            ``,
            `Твой заказ <b>#${orderNo}</b> аннулирован из-за отсутствия подтверждения оплаты.`,
          ].join("\n"),
          {
            parse_mode: "HTML",
            disable_web_page_preview: true,
          }
        );
      }
    } catch (notifyErr) {
      console.error("annulOrderBecauseNoPaymentConfirm notify client error:", notifyErr);
    }

    return { ok: true, order: freshOrder };
  } catch (e) {
    console.error("annulOrderBecauseNoPaymentConfirm error:", e);
    return { ok: false, reason: "INTERNAL_ERROR" };
  }
}

async function processOrdersWithoutPaymentConfirm() {
  try {
    const timeoutMinutes = Number(process.env.ORDER_PAYMENT_CONFIRM_TIMEOUT_MINUTES || 10);
    const cutoff = new Date(Date.now() - timeoutMinutes * 60 * 1000);

    const staleOrders = await Order.find({
      status: { $in: ["created", "processing"] },
      "payment.status": "unpaid",
      createdAt: { $lte: cutoff },
    });

    for (const order of staleOrders) {
      await annulOrderBecauseNoPaymentConfirm(order, {
        reason: "NO_PAYMENT_CONFIRM_TIMEOUT",
      });
    }
  } catch (e) {
    console.error("processOrdersWithoutPaymentConfirm error:", e);
  }
}

// async function updateManagerOrderChannelMessage(order, options = {}) {
//   try {
//     if (!bot || !order) return false;

//     const messageChatId = String(
//       order?.payment?.managerMessageChatId ||
//       order?.managerMessageChatId ||
//       order?.managerChannelChatId ||
//       order?.notificationChatId ||
//       order?.managerNotificationChatId ||
//       order?.managerChatId ||
//       ""
//     ).trim();

//     const messageId = Number(
//       order?.payment?.managerMessageId ||
//       order?.managerMessageId ||
//       order?.managerChannelMessageId ||
//       order?.notificationMessageId ||
//       order?.managerNotificationMessageId ||
//       order?.managerMsgId ||
//       0
//     );

//     console.log("[MANAGER MSG UPDATE]", {
//       orderId: String(order?._id || ""),
//       messageChatId,
//       messageId,
//       paymentManagerMessageChatId: order?.payment?.managerMessageChatId,
//       paymentManagerMessageId: order?.payment?.managerMessageId,
//       managerMessageChatId: order?.managerMessageChatId,
//       managerMessageId: order?.managerMessageId,
//     });

//     if (!messageChatId || !messageId) return false;

//     const cancelSource = String(options?.cancelSource || "").trim().toLowerCase();

//     const statusLabel =
//       cancelSource === "client"
//         ? "❌ Отменен клиентом"
//         : cancelSource === "manager"
//         ? "❌ Отменен менеджером"
//         : "❌ Отменен";

//     const dateText = formatOrderDate(order?.createdAt || new Date());
//     const orderNo = escapeHtml(order?.orderNo || order?._id || "Заказ");
//     const totalText = Number(order?.totalZl || 0).toFixed(2);

//     const deliveryType = String(order?.deliveryType || "").trim();
//     const deliveryMethod = String(order?.deliveryMethod || "").trim();

//     const pickupTitle = escapeHtml(
//       order?.pickupPointTitle || order?.pickupPointAddress || ""
//     );
//     const courierAddress = escapeHtml(order?.courierAddress || "");
//     const arrivalTime = escapeHtml(order?.arrivalTime || "");
//     const inpostData = order?.inpostData || {};

//     const userName = escapeHtml(
//       order?.userSnapshot?.displayName ||
//       order?.userSnapshot?.firstName ||
//       order?.userFirstName ||
//       order?.userName ||
//       "Клиент"
//     );

//     const userTelegramId = escapeHtml(order?.userTelegramId || "");

//     const lines = [];
//     lines.push(`🧾 <b>Заказ ${orderNo}</b>`);
//     lines.push(`Статус: <b>${statusLabel}</b>`);
//     lines.push(`Создан: <b>${dateText}</b>`);
//     lines.push(
//       `Клиент: <b>${userName}</b>${userTelegramId ? ` (${userTelegramId})` : ""}`
//     );
//     lines.push(`Сумма: <b>${totalText} zł</b>`);

//     if (deliveryType === "pickup") {
//       lines.push(`Получение: <b>Самовывоз</b>${pickupTitle ? ` — ${pickupTitle}` : ""}`);
//       if (arrivalTime) lines.push(`Время прибытия: <b>${arrivalTime}</b>`);
//     } else if (deliveryType === "delivery") {
//       if (deliveryMethod === "inpost") {
//         lines.push(`Получение: <b>Доставка · InPost</b>`);

//         const fullName = escapeHtml(inpostData?.fullName || "");
//         const phone = escapeHtml(inpostData?.phone || "");
//         const email = escapeHtml(inpostData?.email || "");
//         const city = escapeHtml(inpostData?.city || "");
//         const lockerAddress = escapeHtml(inpostData?.lockerAddress || "");

//         if (fullName) lines.push(`Имя: <b>${fullName}</b>`);
//         if (phone) lines.push(`Телефон: <b>${phone}</b>`);
//         if (email) lines.push(`Email: <b>${email}</b>`);
//         if (city) lines.push(`Город: <b>${city}</b>`);
//         if (lockerAddress) lines.push(`Пачкомат: <b>${lockerAddress}</b>`);
//       } else {
//         lines.push(`Получение: <b>Доставка · Курьер</b>`);
//         if (courierAddress) lines.push(`Адрес: <b>${courierAddress}</b>`);
//       }
//     }

//     const items = Array.isArray(order?.items) ? order.items : [];
//     if (items.length) {
//       lines.push("");
//       lines.push("<b>Позиции:</b>");

//       for (const item of items) {
//         const productTitle = escapeHtml(
//           item?.productTitle1 ||
//           item?.productTitle ||
//           item?.title ||
//           item?.productKey ||
//           "Товар"
//         );

//         const flavorRows = Array.isArray(item?.flavors) ? item.flavors : [];

//         if (flavorRows.length) {
//           for (const fl of flavorRows) {
//             const flavorLabel = escapeHtml(
//               fl?.flavorLabel || fl?.label || fl?.flavorKey || "Вкус"
//             );
//             const qty = Math.max(1, Number(fl?.qty || 1));
//             const unitPrice = Number(fl?.unitPrice || item?.unitPrice || 0).toFixed(2);

//             lines.push(`• ${productTitle} — ${flavorLabel} × ${qty} (${unitPrice} zł)`);
//           }
//         } else {
//           const flavorLabel = escapeHtml(item?.flavorLabel || item?.flavorKey || "");
//           const qty = Math.max(1, Number(item?.qty || 1));
//           const unitPrice = Number(item?.unitPrice || 0).toFixed(2);

//           lines.push(
//             `• ${productTitle}${flavorLabel ? ` — ${flavorLabel}` : ""} × ${qty} (${unitPrice} zł)`
//           );
//         }
//       }
//     }

//   const nextText = lines.join("\n");

//   try {
//     await bot.telegram.editMessageText(
//       messageChatId,
//       messageId,
//       undefined,
//       nextText,
//       {
//         parse_mode: "HTML",
//         disable_web_page_preview: true,
//         reply_markup: { inline_keyboard: [] },
//       }
//     );
//   } catch (editTextErr) {
//     console.error("updateManagerOrderChannelMessage editMessageText error:", editTextErr);

//     try {
//       await bot.telegram.editMessageReplyMarkup(messageChatId, messageId, undefined, {
//         inline_keyboard: [],
//       });
//     } catch (editMarkupErr) {
//       console.error("updateManagerOrderChannelMessage editMessageReplyMarkup error:", editMarkupErr);
//     }

//     try {
//       await bot.telegram.sendMessage(messageChatId, nextText, {
//         parse_mode: "HTML",
//         disable_web_page_preview: true,
//       });
//     } catch (sendFallbackErr) {
//       console.error("updateManagerOrderChannelMessage fallback sendMessage error:", sendFallbackErr);
//     }
//   }

//     return true;
//   } catch (e) {
//     console.error("updateManagerOrderChannelMessage error:", e);
//     return false;
//   }
// }

const DAILY_STATS_RUNTIME_SENT = new Set();

function getPointStatsChatId(point) {
  return String(point?.statsChatId || "").trim();
}

function getWarsawDayKey(dateLike = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Warsaw",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date(dateLike));

  const year = parts.find((p) => p.type === "year")?.value || "0000";
  const month = parts.find((p) => p.type === "month")?.value || "00";
  const day = parts.find((p) => p.type === "day")?.value || "00";

  return `${year}-${month}-${day}`;
}

function getWarsawTimeHHMM(dateLike = new Date()) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Warsaw",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date(dateLike));

  const hour = parts.find((p) => p.type === "hour")?.value || "00";
  const minute = parts.find((p) => p.type === "minute")?.value || "00";

  return `${hour}:${minute}`;
}

function getPointStatsSendTime(point, dateLike = new Date()) {
  const todayKey = getWarsawDayKey(dateLike);

  const rawSchedule =
    point?.scheduleByDate?.[todayKey] ||
    point?.scheduleByDate?.get?.(todayKey) ||
    null;

  if (rawSchedule?.isOpen === false) {
    return null;
  }

  const raw = String(
    rawSchedule?.to ||
      point?.statsSendTime ||
      point?.workEnd ||
      point?.closeTime ||
      point?.workingHours?.to ||
      point?.schedule?.endTime ||
      "23:59"
  ).trim();

  return /^([01]\d|2[0-3]):([0-5]\d)$/.test(raw) ? raw : "23:59";
}

function getOrderPointMatch(point) {
  const pointKey = String(point?.key || "").trim().replace(/,+$/, "");

  if (pointKey === "delivery") {
    return { deliveryType: "delivery", deliveryMethod: "courier" };
  }

  if (pointKey === "delivery-2") {
    return { deliveryType: "delivery", deliveryMethod: "inpost" };
  }

  return {
    deliveryType: "pickup",
    pickupPointId: point?._id,
  };
}

function shouldCountOrderInDailyStats(order) {
  if (!order) return false;

  const status = String(order?.status || "").trim().toLowerCase();
  const paymentStatus = String(order?.payment?.status || "").trim().toLowerCase();

  if (["canceled", "annulled"].includes(status)) return false;
  if (order?.payment?.cashbackFullyPaid === true) return true;
  if (["paid", "refunded"].includes(paymentStatus)) return true;
  if (["processing", "done", "completed", "shipped", "assembled"].includes(status)) return true;

  return false;
}

function allocateCashbackBySubtotal(orderTotal, orderCashback, itemSubtotal) {
  const total = Number(orderTotal || 0);
  const cashback = Number(orderCashback || 0);
  const subtotal = Number(itemSubtotal || 0);

  if (total <= 0 || cashback <= 0 || subtotal <= 0) return 0;
  return Number(((cashback * subtotal) / total).toFixed(2));
}

function formatPaymentMethodLabel(method) {
  const key = String(method || "").trim().toLowerCase();

  if (key === "cash") return "Наличные";
  if (key === "blik") return "BLIK";
  if (key === "crypto") return "Крипта";
  if (key === "ua_card") return "Укр. карта";
  if (key === "cashback") return "Кэшбек";

  return key || "Не указан";
}

function getOrderDisplayedPaymentMethod(order) {
  const paymentMethod = String(order?.payment?.method || "").trim();

  if (paymentMethod) return paymentMethod;
  if (order?.payment?.cashbackFullyPaid === true) return "cashback";

  return "unknown";
}

function getOrderRowUnitBasePrice(row, productBasePriceMap) {
  const flavors = Array.isArray(row?.flavors) ? row.flavors : [];

  const savedBasePrice = flavors.length
    ? Math.max(...flavors.map((f) => Number(f?.baseUnitPrice || 0)))
    : 0;

  if (savedBasePrice > 0) return savedBasePrice;

  const productKey = String(row?.productKey || "").trim();
  const basePrice = Number(productBasePriceMap.get(productKey) || 0);

  if (basePrice > 0) return basePrice;

  const fallback = flavors.length
    ? Math.max(...flavors.map((f) => Number(f?.unitPrice || 0)))
    : 0;

  return Number(fallback || 0);
}

function buildDailyStatsMessage(point, orders, dayKey, extra = {}) {
  const productBasePriceMap =
    extra?.productBasePriceMap instanceof Map ? extra.productBasePriceMap : new Map();

  const referredFirstOrderUsers =
    extra?.referredFirstOrderUsers instanceof Set ? extra.referredFirstOrderUsers : new Set();

  const userDisplayMap =
    extra?.userDisplayMap instanceof Map ? extra.userDisplayMap : new Map();

  function getProductBucketLabelByQty(qty) {
    const n = Math.max(0, Number(qty || 0));
    if (n >= 5) return "5шт.";
    if (n >= 3) return "3-4шт.";
    if (n >= 2) return "2шт.";
    return "1шт.";
  }

  function getProductDisplayTitleForStats(productRow = {}) {
    const t1 = String(productRow?.productTitle1 || "").trim();
    const t2 = String(productRow?.productTitle2 || "").trim();
    return [t1, t2].filter(Boolean).join(" ").trim() || "Товар";
  }

  // function getOrderOriginalItemsTotalZl(order) {
  //   const items = Array.isArray(order?.items) ? order.items : [];

  //   const itemsTotal = items.reduce((sum, productRow) => {
  //     const flavors = Array.isArray(productRow?.flavors) ? productRow.flavors : [];
  //     const productQty = flavors.reduce(
  //       (acc, flavor) => acc + Math.max(1, Number(flavor?.qty || 1)),
  //       0
  //     );

  //     const productKey = String(productRow?.productKey || "").trim();

  //     let productBasePrice = Number(
  //       productRow?.productBasePrice ||
  //         productRow?.basePrice ||
  //         productBasePriceMap.get(productKey) ||
  //         productRow?.price ||
  //         0
  //     );

  //     if (!productBasePrice && flavors.length) {
  //       const flavorBasePrices = flavors
  //         .map((flavor) =>
  //           Number(
  //             flavor?.basePrice ||
  //               flavor?.baseUnitPrice ||
  //               flavor?.originalUnitPrice ||
  //               0
  //           )
  //         )
  //         .filter((value) => value > 0);

  //       if (flavorBasePrices.length) {
  //         productBasePrice = Math.max(...flavorBasePrices);
  //       }
  //     }

  //     if (!productBasePrice && flavors.length) {
  //       const flavorUnitPrices = flavors
  //         .map((flavor) => Number(flavor?.unitPrice || 0))
  //         .filter((value) => value > 0);

  //       if (flavorUnitPrices.length) {
  //         productBasePrice = Math.max(...flavorUnitPrices);
  //       }
  //     }

  //     return sum + productQty * productBasePrice;
  //   }, 0);

  //   return Number(itemsTotal.toFixed(2));
  // }

  function getOrderSmartDiscountTotalZl(order) {
    return Number(
      (Array.isArray(order?.items) ? order.items : []).reduce((orderSum, item) => {
        const flavors = Array.isArray(item?.flavors) ? item.flavors : [];

        return (
          orderSum +
          flavors.reduce((flavorSum, flavor) => {
            const explicitSmartDiscount = Number(flavor?.smartDiscountTotalZl || 0);
            if (explicitSmartDiscount > 0) {
              return flavorSum + explicitSmartDiscount;
            }

            const qty = Math.max(1, Number(flavor?.qty || 1));
            const originalBasePrice = Number(flavor?.baseUnitPrice || 0);
            const finalUnitPrice = Number(flavor?.unitPrice || 0);
            const referralDiscountTotal = Number(flavor?.referralFirstOrderDiscountTotalZl || 0);

            if (originalBasePrice <= 0 || finalUnitPrice <= 0) {
              return flavorSum;
            } 
            

            const totalPriceDelta = Math.max(0, (originalBasePrice - finalUnitPrice) * qty);
            const smartOnlyDiscount = Math.max(0, totalPriceDelta - referralDiscountTotal);

            return flavorSum + smartOnlyDiscount;
          }, 0)
        );
      }, 0).toFixed(2)
    );
  }

  function getOrderCashbackDiscountTotalZl(order) {
    return Number(order?.payment?.cashbackAppliedZl || 0);
  }

  const orderBlocks = [];
  let soldPositionsQty = 0;

  for (const order of Array.isArray(orders) ? orders : []) {
    const orderCashbackSpent = getOrderCashbackDiscountTotalZl(order);
    const orderSmartDiscountZl = getOrderSmartDiscountTotalZl(order);
    const paymentMethod = getOrderDisplayedPaymentMethod(order);
    const paymentMethodLabel = formatPaymentMethodLabel(paymentMethod);

    const orderClientName =
      userDisplayMap.get(String(order?.userTelegramId || "").trim()) ||
      String(order?.userTelegramId || "Клиент");

    const productLines = (Array.isArray(order?.items) ? order.items : []).flatMap((productRow) => {
      const flavors = Array.isArray(productRow?.flavors) ? productRow.flavors : [];
      const productQty = flavors.reduce(
        (acc, flavor) => acc + Math.max(1, Number(flavor?.qty || 1)),
        0
      );

      if (productQty <= 0) return [];

      soldPositionsQty += productQty;

      const productTitle = getProductDisplayTitleForStats(productRow);
      const bucketLabel = getProductBucketLabelByQty(productQty);
      const flavorsLine = flavors
        .map((flavor) => {
          const label =
            String(flavor?.flavorLabel || flavor?.label || flavor?.flavorKey || "").trim() ||
            "Вкус";
          const qty = Math.max(1, Number(flavor?.qty || 1));
          return `${escapeHtml(label)} ×${qty}`;
        })
        .join(" • ");

      const flavorsLineWithBullet = flavorsLine ? `• ${flavorsLine}` : "";

      return [
        `<b>${escapeHtml(productTitle)}</b> [${bucketLabel}] - ${productQty}шт.`,
        flavorsLineWithBullet,
      ];
    });

    orderBlocks.push({
      orderNo: String(order?.orderNo || "—"),
      clientName: orderClientName,
      paymentMethodLabel,
      smartDiscountZl: Number(orderSmartDiscountZl.toFixed(2)),
      cashbackSpentZl: Number(orderCashbackSpent.toFixed(2)),
      lines: productLines,
      createdAt: order?.createdAt || null,
    });
  }

  const uniqueCustomersCount = new Set(
    (Array.isArray(orders) ? orders : [])
      .map((order) => String(order?.userTelegramId || "").trim())
      .filter(Boolean)
  ).size;

  const toPlnFromOrder = (order) => {
    const payment = order?.payment || {};

    const managerDisplayCurrency = String(payment?.managerDisplayCurrency || "PLN")
      .trim()
      .toUpperCase();

    const managerDisplayAmount = Number(payment?.managerDisplayAmount || 0);
    const managerDisplayRate = Number(payment?.managerDisplayRate || 0);
    const cashbackRemainingToPayZl = Number(payment?.cashbackRemainingToPayZl || 0);
    const totalZl = Number(order?.totalZl || 0);

    // 1) Если есть остаток к оплате в PLN после кэшбека — это самый надёжный вариант
    if (cashbackRemainingToPayZl > 0) {
      return cashbackRemainingToPayZl;
    }

    // 2) Если валюта отображения PLN
    if (managerDisplayCurrency === "PLN") {
      if (managerDisplayAmount > 0) return managerDisplayAmount;
      return totalZl;
    }

    // 3) Если валюта отображения UAH
    // managerDisplayAmount = PLN * rate
    // значит обратно в PLN => UAH / rate
    if (managerDisplayCurrency === "UAH") {
      if (managerDisplayAmount > 0 && managerDisplayRate > 0) {
        return Number((managerDisplayAmount / managerDisplayRate).toFixed(2));
      }
      return totalZl;
    }

    // 4) Если валюта отображения USDT
    // managerDisplayAmount = PLN / rate
    // значит обратно в PLN => USDT * rate
    if (managerDisplayCurrency === "USDT") {
      if (managerDisplayAmount > 0 && managerDisplayRate > 0) {
        return Number((managerDisplayAmount * managerDisplayRate).toFixed(2));
      }
      return totalZl;
    }

    // 5) fallback
    return totalZl;
  };

  const kasaTotalZl = Number(
    (Array.isArray(orders) ? orders : [])
      .reduce((sum, order) => {
        return sum + toPlnFromOrder(order);
      }, 0)
      .toFixed(2)
  );

  const smartDiscountTotalZl = Number(
    (Array.isArray(orders) ? orders : [])
      .reduce((sum, order) => sum + getOrderSmartDiscountTotalZl(order), 0)
      .toFixed(2)
  );

  const referralDiscountTotalZl = Number(
    (Array.isArray(orders) ? orders : [])
      .reduce((sum, order) => sum + Number(order?.payment?.referralFirstOrderDiscountTotalZl || 0), 0)
      .toFixed(2)
  );

  const cashbackDiscountTotalZl = Number(
    (Array.isArray(orders) ? orders : [])
      .reduce((sum, order) => sum + getOrderCashbackDiscountTotalZl(order), 0)
      .toFixed(2)
  );

  const courierDeliveryFeesTotalZl = Number(
    (Array.isArray(orders) ? orders : [])
      .reduce((sum, order) => {
        const isCourierDelivery =
          String(order?.deliveryType || "").trim() === "delivery" &&
          String(order?.deliveryMethod || "").trim() === "courier";

        return sum + (isCourierDelivery ? Number(order?.deliveryFeeZl || 0) : 0);
      }, 0)
      .toFixed(2)
  );

  const inpostDeliveryFeesTotalZl = Number(
    (Array.isArray(orders) ? orders : [])
      .reduce((sum, order) => {
        const isInpostDelivery =
          String(order?.deliveryType || "").trim() === "delivery" &&
          String(order?.deliveryMethod || "").trim() === "inpost";

        return sum + (isInpostDelivery ? Number(order?.inpostDeliveryFeeZl || 0) : 0);
      }, 0)
      .toFixed(2)
  );

  const discountsTotalZl = Number(
    (smartDiscountTotalZl + referralDiscountTotalZl + cashbackDiscountTotalZl).toFixed(2)
  );
  
  const salaryTotalZl = Number((((kasaTotalZl / 100) * 16)).toFixed(2));

  const pointTitle = point?.title || point?.address || point?.key || "Склад";
  const sortedOrders = [...orderBlocks].sort(
    (a, b) => new Date(a.createdAt || 0).getTime() - new Date(b.createdAt || 0).getTime()
  );

  const lines = [
    `📊 <b>СТАТИСТИКА ДНЯ</b>`,
    `🏪 Склад: ${escapeHtml(pointTitle)}`,
    `📅 Дата: ${escapeHtml(dayKey)}`,
    `——————————————————`,
    `🧾 <b>ЗАКАЗЫ :</b>`,
    ``,
  ];

  const productStatsMap = new Map();

  for (const order of Array.isArray(orders) ? orders : []) {
    for (const row of Array.isArray(order?.items) ? order.items : []) {
      const productKey = String(row?.productKey || "").trim();
      const productTitle =
        [row?.productTitle1, row?.productTitle2]
          .filter(Boolean)
          .join(" ")
          .trim() || productKey || "Товар";

      const statsKey = productKey || productTitle;
      if (!statsKey) continue;

      let bucket = productStatsMap.get(statsKey);
      if (!bucket) {
        bucket = {
          title: productTitle,
          totalQty: 0,
          tierBuckets: new Map(),
          flavors: new Map(),
        };
        productStatsMap.set(statsKey, bucket);
      }

      const flavors = Array.isArray(row?.flavors) ? row.flavors : [];

      for (const flavor of flavors) {
        const qty = Math.max(0, Number(flavor?.qty || 0));
        if (!qty) continue;

        bucket.totalQty += qty;

        const smartDiscountPerItem = Number(flavor?.smartDiscountPerItem || 0);

        const tierLabel = (() => {
          if (smartDiscountPerItem >= 15) return "[5]";
          if (smartDiscountPerItem >= 10) return "[3-4]";
          if (smartDiscountPerItem >= 5) return "[2]";
          return "[1]";
        })();

        bucket.tierBuckets.set(
          tierLabel,
          (bucket.tierBuckets.get(tierLabel) || 0) + qty
        );

        const flavorLabel = String(
          flavor?.flavorLabel || flavor?.flavorKey || "Вкус"
        ).trim();

        if (flavorLabel) {
          bucket.flavors.set(
            flavorLabel,
            (bucket.flavors.get(flavorLabel) || 0) + qty
          );
        }
      }
    }
  }

  const tierOrder = ["[5]", "[3-4]", "[2]", "[1]"];

  const aggregatedProducts = Array.from(productStatsMap.values()).sort(
    (a, b) => b.totalQty - a.totalQty || a.title.localeCompare(b.title, "ru")
  );

  if (!aggregatedProducts.length) {
    lines.push("—");
    lines.push("");
  } else {
    for (const product of aggregatedProducts) {
      lines.push(`——————————————————`);
      lines.push(`🦆 <b>${escapeHtml(product.title)} — ${product.totalQty} шт.</b>`);
      lines.push("");

      const tierLine = tierOrder
        .filter((tier) => (product.tierBuckets.get(tier) || 0) > 0)
        .map((tier) => `${tier} ${product.tierBuckets.get(tier)}`)
        .join(" &lt;&gt; ");

      if (tierLine) {
        lines.push(tierLine);
      }

      lines.push("");
      // lines.push("Вкусы:");

      const sortedFlavors = Array.from(product.flavors.entries()).sort(
        (a, b) => b[1] - a[1] || a[0].localeCompare(b[0], "ru")
      );

      for (const [flavorLabel, qty] of sortedFlavors) {
        lines.push(`${flavorLabel} ×${qty}`);
      }
      lines.push(`——————————————————`);

      lines.push("");
    }
  }

  /*
  if (!sortedOrders.length) {
    lines.push(`Заказов за день не было.`);
  } else {
    sortedOrders.forEach((order, index) => {
      lines.push(`#${escapeHtml(order.orderNo)}  [${escapeHtml(order.clientName)}]`);
      for (const line of order.lines) {
        lines.push(line);
      }

      if (index !== sortedOrders.length - 1) {
        lines.push(``);
      }
    });
  }
  */

  lines.push(`——————————————————`);
  lines.push(`💰Касса: ${kasaTotalZl.toFixed(2)} PLN`);

  const pointKeyNorm = String(point?.key || "").trim().toLowerCase().replace(/,+$/, "");
  const pointTitleNorm = normalizePhotoLookupText(point?.title || "");
  const pointAddressNorm = normalizePhotoLookupText(point?.address || "");

  const isCourierStatsPoint =
    pointKeyNorm === "delivery" ||
    pointTitleNorm.includes("kurier") ||
    pointTitleNorm.includes("courier") ||
    pointAddressNorm.includes("kurier") ||
    pointAddressNorm.includes("courier");

  if (isCourierStatsPoint) {
    lines.push(`🚚Доставка: ${courierDeliveryFeesTotalZl.toFixed(2)} PLN`);
  }

  const isInpostStatsPoint =
    pointKeyNorm === "delivery-2" ||
    pointTitleNorm.includes("inpost") ||
    pointAddressNorm.includes("inpost");

  if (isInpostStatsPoint) {
    lines.push(`📦Доставка InPost: ${inpostDeliveryFeesTotalZl.toFixed(2)} PLN`);
  }

  lines.push(`🪙Скидки: ${discountsTotalZl.toFixed(2)} PLN`);
  lines.push(`- по ⚙️смарт-цене: ${smartDiscountTotalZl.toFixed(2)} PLN`);
  lines.push(`- по 🎁реф. скидке: ${referralDiscountTotalZl.toFixed(2)} PLN`);
  lines.push(`- по 🪙кэшбеку: ${cashbackDiscountTotalZl.toFixed(2)} PLN`);
  lines.push(`👨‍💼Зарплата: ${salaryTotalZl.toFixed(2)} PLN`);
  lines.push(`🫂Рефералов: ${referredFirstOrderUsers.size}`);
  lines.push(`👤Кол-во клиентов: ${uniqueCustomersCount}`);
  lines.push(`⚙️Продано штук: ${soldPositionsQty}`);
  lines.push(`——————————————————`);
  lines.push(`🦆 ELF DUCK &lt;&gt; СТАТИСТИКА`);

  return lines.join("\n");
}

async function sendDailyPointStats(point, orders, dayKey, extra = {}) {
  try {
    if (!bot || !point) return { ok: false, reason: "NO_BOT_OR_POINT" };

    let chatId = getPointStatsChatId(point);
    if (!chatId) return { ok: false, reason: "NO_STATS_CHAT" };

    const fullText = buildDailyStatsMessage(point, orders, dayKey, extra);

    const splitTelegramHtmlMessage = (text, maxLen = 3500) => {
      const src = String(text || "");
      if (!src) return [""];

      const lines = src.split("\n");
      const chunks = [];
      let current = "";

      const pushCurrent = () => {
        if (current) chunks.push(current);
        current = "";
      };

      for (const line of lines) {
        const candidate = current ? `${current}\n${line}` : line;

        if (candidate.length <= maxLen) {
          current = candidate;
          continue;
        }

        if (current) pushCurrent();

        if (line.length <= maxLen) {
          current = line;
          continue;
        }

        let rest = line;
        while (rest.length > maxLen) {
          chunks.push(rest.slice(0, maxLen));
          rest = rest.slice(maxLen);
        }
        current = rest;
      }

      pushCurrent();
      return chunks.length ? chunks : [src.slice(0, maxLen)];
    };

    const parts = splitTelegramHtmlMessage(fullText, 3500);

    const sendAllParts = async (targetChatId) => {
      for (const part of parts) {
        await bot.telegram.sendMessage(targetChatId, part, {
          parse_mode: "HTML",
          disable_web_page_preview: true,
        });
      }
    };

    try {
      await sendAllParts(chatId);
      return { ok: true, parts: parts.length };
    } catch (e) {
      const migratedChatId = e?.response?.parameters?.migrate_to_chat_id;
      if (!migratedChatId) throw e;

      const nextChatId = String(migratedChatId).trim();

      await PickupPoint.updateOne(
        { _id: point._id },
        { $set: { statsChatId: nextChatId } }
      );

      chatId = nextChatId;
      await sendAllParts(chatId);

      return {
        ok: true,
        migrated: true,
        chatId,
        parts: parts.length,
      };
    }
  } catch (e) {
    console.error("sendDailyPointStats error:", e);
    return { ok: false, reason: "SEND_ERROR" };
  }
}

async function processDailyPointStats() {
  try {
    const now = new Date();
    const nowHHMM = getWarsawTimeHHMM(now);
    const dayKey = getWarsawDayKey(now);
    const ordersSince = new Date(Date.now() - 48 * 60 * 60 * 1000);

    const points = await PickupPoint.find(
      {
        $or: [
          { statsChatId: { $exists: true, $ne: "" } },
          { notificationChatId: { $exists: true, $ne: "" } },
        ],
      },
      {
        _id: 1,
        key: 1,
        title: 1,
        address: 1,
        notificationChatId: 1,
        statsChatId: 1,
        statsSendTime: 1,
        managerSalaryPercent: 1,
        scheduleByDate: 1,
        workEnd: 1,
        closeTime: 1,
        workingHours: 1,
        schedule: 1,
      }
    ).lean();

    for (const point of points) {
      const sendTime = getPointStatsSendTime(point, now);
      if (!sendTime) continue;
      if (nowHHMM < sendTime) continue;

      const dedupeKey = `${String(point?._id || "")}:${dayKey}`;
      if (DAILY_STATS_RUNTIME_SENT.has(dedupeKey)) continue;

      const match = getOrderPointMatch(point);
      const orders = await Order.find( //wefv
        {
          ...match,
          createdAt: { $gte: ordersSince },
          status: { $ne: "canceled" },
        },
        {
          userTelegramId: 1,
          orderNo: 1,
          totalZl: 1,
          status: 1,
          payment: 1,
          items: 1,
          cashbackZl: 1,
          createdAt: 1,
          deliveryType: 1,
          deliveryMethod: 1,
          deliveryFeeZl: 1,
          inpostDeliveryFeeZl: 1,
        }
      ).lean();

      const dayOrders = orders.filter((order) => {
        if (getWarsawDayKey(order?.createdAt) !== dayKey) return false;
        return shouldCountOrderInDailyStats(order);
      });

      const needsFallbackBasePrices = dayOrders.some((order) =>
        (Array.isArray(order?.items) ? order.items : []).some((row) => {
          const flavors = Array.isArray(row?.flavors) ? row.flavors : [];
          return !flavors.some((f) => Number(f?.baseUnitPrice || 0) > 0);
        })
      );

      const productKeys = needsFallbackBasePrices
        ? Array.from(
            new Set(
              dayOrders.flatMap((order) =>
                (Array.isArray(order?.items) ? order.items : [])
                  .map((row) => String(row?.productKey || "").trim())
                  .filter(Boolean)
              )
            )
          )
        : [];

      const products = productKeys.length
        ? await Product.find(
            { productKey: { $in: productKeys } },
            { productKey: 1, price: 1 }
          ).lean()
        : [];

      const productBasePriceMap = new Map(
        products.map((p) => [String(p?.productKey || "").trim(), Number(p?.price || 0)])
      );

      const orderUserIds = Array.from(
        new Set(dayOrders.map((order) => String(order?.userTelegramId || "").trim()).filter(Boolean))
      );

      const statsUsers = orderUserIds.length
        ? await User.find(
            {
              telegramId: { $in: orderUserIds },
            },
            { telegramId: 1, username: 1, firstName: 1, referral: 1 }
          ).lean()
        : [];

      const referredFirstOrderUsers = new Set(
        statsUsers
          .filter(
            (u) =>
              String(u?.referral?.usedCode || "").trim() &&
              u?.referral?.firstOrderDoneAt &&
              getWarsawDayKey(u?.referral?.firstOrderDoneAt) === dayKey
          )
          .map((u) => String(u?.telegramId || "").trim())
          .filter(Boolean)
      );

      const userDisplayMap = new Map(
        statsUsers.map((u) => {
          const name = String(u?.username || "").trim()
            ? `@${String(u.username).trim()}`
            : String(u?.firstName || "").trim() || String(u?.telegramId || "Клиент");

          return [String(u?.telegramId || "").trim(), name];
        })
      );

      const sent = await sendDailyPointStats(point, dayOrders, dayKey, {
        productBasePriceMap,
        referredFirstOrderUsers,
        userDisplayMap,
      });

      if (sent?.ok) {
        DAILY_STATS_RUNTIME_SENT.add(dedupeKey);
      }
    }
  } catch (e) {
    console.error("processDailyPointStats error:", e);
  }
}

async function notifyManagerDeliveryReadyToShip(order) {
  try {
    if (!bot || !order) return { ok: false, reason: "NO_BOT_OR_ORDER" };

    const point = await resolveOrderNotificationPoint(order);
    const chatId = String(
      order?.payment?.managerMessageChatId || point?.notificationChatId || ""
    ).trim();

    const replyToMessageId = Number(order?.payment?.managerMessageId || 0);

    if (!chatId || !replyToMessageId) {
      return { ok: false, reason: "NO_MANAGER_MESSAGE" };
    }

    const orderNo = escapeHtml(order?.orderNo || "—");
    const deliveryLabel =
      String(order?.deliveryMethod || "").trim() === "inpost"
        ? "с помощью пачкомата"
        : "курьеру";

    const text = [
      `📦 <b>ЗАКАЗ ГОТОВ К ОТПРАВКЕ</b>`,
      ``,
      `Когда вы отдадите ${deliveryLabel} этот заказ (<b>#${orderNo}</b>) нажмите кнопку ниже.`,
    ].join("\n");

    const sent = await bot.telegram.sendMessage(chatId, text, {
      parse_mode: "HTML",
      reply_to_message_id: replyToMessageId,
      allow_sending_without_reply: true,
      reply_markup: {
        inline_keyboard: [
          [{ text: "📦 ЗАКАЗ ОТПРАВЛЕН", callback_data: `mgr_order_shipped:${order._id}` }],
        ],
      },
    });

    await Order.updateOne(
      { _id: order._id },
      {
        $push: {
          managerDeliveryMessageIds: String(sent?.message_id || ""),
        },
      }
    );

    return { ok: true };
  } catch (e) {
    console.error("notifyManagerDeliveryReadyToShip error:", e);
    return { ok: false, reason: "SEND_ERROR" };
  }
}

async function resolveOrderPaymentPoint(order) {
  if (!order) return null;

  if (order.deliveryType === "pickup" && order.pickupPointId) {
    return await PickupPoint.findById(order.pickupPointId).lean();
  }

  if (order.deliveryType === "delivery") {
    const deliveryKey = order.deliveryMethod === "inpost" ? "delivery-2" : "delivery";
    return await PickupPoint.findOne({
      key: { $in: [deliveryKey, `${deliveryKey},`] }
    }).lean();
  }

  return null;
}

async function sendOrderCreatedNotification(order) {
  try {
    if (!bot || !order) return;

    const point = await resolveOrderNotificationPoint(order);
    if (!point?.notificationChatId) return;

    const user = await User.findOne(
      { telegramId: String(order.userTelegramId || "") },
      { telegramId: 1, username: 1, firstName: 1 }
    ).lean();

    const customerName =
      (user?.username ? `@${user.username}` : "") ||
      String(user?.firstName || "").trim() ||
      "—";

    const managerAmountValue = Number(order?.payment?.managerDisplayAmount || 0);
    const managerAmountCurrency = String(order?.payment?.managerDisplayCurrency || "").trim();

    const managerAmountText =
      managerAmountValue > 0 && managerAmountCurrency
        ? `${managerAmountValue.toFixed(2)} ${escapeHtml(managerAmountCurrency)}`
        : `${Number(order.totalZl || 0)} ${escapeHtml(order.currency || "PLN")}`;

    const itemsText = (order.items || [])
      .map((it) => {
        const productTitle =
          [it.productTitle1, it.productTitle2].filter(Boolean).join(" ").trim() ||
          it.productKey ||
          "Товар";

        const flavorsText = (it.flavors || [])
          .map((f) => {
            const flavor = f.flavorLabel || f.flavorKey || "Вкус";
            const priceText = Number(f.unitPrice || 0) > 0
              ? ` • ${Number(f.unitPrice || 0)} zł/шт.`
              : "";
            return `• ${escapeHtml(flavor)} — ${Number(f.qty || 0)} шт.${priceText}`;
          })
          .join("\n");

        return `📦 <b>${escapeHtml(productTitle)}</b>\n${flavorsText}`;
      })
      .join("\n\n");

    const paymentMethodLabel =
      order?.payment?.method === "blik"
        ? "BLIK"
        : order?.payment?.method === "crypto"
        ? "Криптовалюта"
        : order?.payment?.method === "ua_card"
        ? "Украинская карта"
        : order?.payment?.method === "cash"
        ? "Наличные"
        : "—";

    const referralFirstOrderDiscountAppliedZl = Number(order?.payment?.referralFirstOrderDiscountTotalZl || 0);
    const referralFirstOrderDiscountPercent = Number(order?.payment?.referralFirstOrderDiscountPercent || 0);
    const referralUsedCode = String(order?.payment?.referralUsedCode || "").trim();
    const hasReferralFirstOrderDiscount = order?.payment?.referralFirstOrderDiscountApplied === true;

    let inviterLabel = "";
    if (referralUsedCode) {
      const inviterUser = await User.findOne(
        { "referral.code": referralUsedCode },
        { username: 1, firstName: 1, telegramId: 1 }
      ).lean();

      inviterLabel = inviterUser?.username
        ? `@${String(inviterUser.username).trim()}`
        : String(inviterUser?.firstName || "").trim() || String(inviterUser?.telegramId || "").trim();
    }

    const lines = [
      `🛒 <b>ОПЛАТА ОТПРАВЛЕНА НА ПРОВЕРКУ</b>`,
      ``,
      `🔢 <b>Номер:</b> #${escapeHtml(order.orderNo)}`,
      ``,
      `👤 <b>Клиент:</b> ${escapeHtml(customerName)}`,
      `🕒 <b>Создан:</b> ${escapeHtml(formatOrderDate(order.createdAt))}`,
      ``,
      `📋 <b>Состав заказа:</b>`,
      ``,
      itemsText || "—",
      ``,
      `💰 <b>Сумма заказа:</b> ${managerAmountText}`,
      `💳 <b>Способ оплаты:</b> ${
        order?.payment?.cashbackFullyPaid
          ? "Кэшбек"
          : escapeHtml(paymentMethodLabel)
      }`,
      ``,
      order?.payment?.cashbackAppliedZl > 0
        ? `🪙 <b>Оплачено кэшбеком:</b> ${Number(order.payment.cashbackAppliedZl || 0).toFixed(2)} ${escapeHtml(order.currency || "PLN")}`
        : null,
      order?.payment?.cashbackAppliedZl > 0 && Number(order?.payment?.cashbackRemainingToPayZl || 0) > 0
        ? `💸 <b>Остаток к оплате:</b> ${Number(order.payment.cashbackRemainingToPayZl || 0).toFixed(2)} ${escapeHtml(order.currency || "PLN")}`
        : null,
      hasReferralFirstOrderDiscount
        ? `🎁 <b>Реферальная скидка:</b> ${referralFirstOrderDiscountPercent || 10}% на первый заказ${referralFirstOrderDiscountAppliedZl > 0 ? ` (${referralFirstOrderDiscountAppliedZl.toFixed(2)} PLN)` : ""}${inviterLabel ? `\n👤 <b>Пригласитель:</b> ${escapeHtml(inviterLabel)}` : ""}`
        : null,
      ``,
      order?.payment?.cashbackFullyPaid
        ? `💳 <b>Статус оплаты:</b> ✅ Полностью оплачено`
        : `💳 <b>Статус оплаты:</b> 🟠 Оплата на проверке`,
      ``,
    ];

    if (order.deliveryType === "pickup" && order.arrivalTime) {
      lines.push(`🚚 <b>Клиент будет в ${escapeHtml(order.arrivalTime)}</b>`);
      lines.push("");
    }

    if (String(order?.deliveryType || "") === "delivery" && String(order?.deliveryMethod || "") === "courier") {
      if (order?.courierAddress) lines.push(`📍 <b>Адрес доставки:</b> ${escapeHtml(order.courierAddress)}`);
      if (order?.courierDistrict) lines.push(`🌍 <b>Район:</b> ${escapeHtml(order.courierDistrict)}`);
      if (Number(order?.deliveryFeeZl || 0) > 0) lines.push(`🚚 <b>Стоимость доставки:</b> ${Number(order.deliveryFeeZl || 0).toFixed(2)} PLN`);
      if (order?.deliveryTimeWindow) lines.push(`🕒 <b>Временной промежуток:</b> ${escapeHtml(order.deliveryTimeWindow)}`);
      lines.push("");
    }

    if (order.deliveryType === "delivery" && order.deliveryMethod === "inpost") {
      if (order.inpostData?.fullName) lines.push(`👤 <b>Получатель:</b> ${escapeHtml(order.inpostData.fullName)}`);
      if (order.inpostData?.phone) lines.push(`📞 <b>Телефон:</b> ${escapeHtml(order.inpostData.phone)}`);
      if (order.inpostData?.email) lines.push(`✉️ <b>Email:</b> ${escapeHtml(order.inpostData.email)}`);
      if (order.inpostData?.city) lines.push(`🏙 <b>Город:</b> ${escapeHtml(order.inpostData.city)}`);
      if (order.inpostData?.lockerAddress) lines.push(`📦 <b>Пачкомат:</b> ${escapeHtml(order.inpostData.lockerAddress)}`);
      if (
        order.inpostData?.fullName ||
        order.inpostData?.phone ||
        order.inpostData?.email ||
        order.inpostData?.city ||
        order.inpostData?.lockerAddress
      ) {
        lines.push("");
      }
      if (Number(order.inpostDeliveryFeeZl || 0) > 0) {
        lines.push(`🚚 <b>Стоимость доставки InPost:</b> ${Number(order.inpostDeliveryFeeZl || 0).toFixed(2)} PLN`);
      }
      if (Number(order.inpostPackageUnits || 0) > 0) {
        lines.push(`📦 <b>Условные единицы:</b> ${Number(order.inpostPackageUnits || 0).toFixed(2)}`);
      }
    }

    if (order.payment?.method === "cash") {
      if (order.payment?.cashChangeType === "need_change" && order.payment?.cashAmount) {
        lines.push(`💵 <b>Сдача:</b> ${escapeHtml(order.payment.cashAmount)} zł`);
        lines.push("");
      } else if (order.payment?.cashChangeType === "no_change") {
        lines.push(`💵 <b>Сдача:</b> без сдачи`);
        lines.push("");
      }
    }

    const text = lines.filter((line) => line !== null && line !== undefined).join("\n");

const initialReplyMarkup =
  String(order?.deliveryType || "") === "pickup" &&
  String(order?.payment?.method || "") === "cash"
    ? {
        inline_keyboard: [
          [
            { text: "🕒 Ожидаю", callback_data: `mgr_pay_paid:${order._id}` },
            { text: "❌ Отклонить", callback_data: `mgr_pay_unpaid:${order._id}` },
          ],
        ],
      }
    : {
        inline_keyboard: [
          [
            { text: "✅ Оплачено", callback_data: `mgr_pay_paid:${order._id}` },
            { text: "❌ Отклонить", callback_data: `mgr_pay_unpaid:${order._id}` },
          ],
        ],
      };
const pickupPoint = order?.pickupPointId
  ? await PickupPoint.findById(order.pickupPointId).lean().catch(() => null)
  : null;

const photoPoint = point || pickupPoint || null;

const managerOrderPhotoUrl = "";
let clientOrderPhotoUrl = firstNonEmptyString(
  getCustomerOrderPhotoByPickupPoint(order, photoPoint),
  getManagerOrderPhotoByPickupPoint(order, photoPoint),
  process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
  process.env.TG_ORDER_PHOTO_DEFAULT
);

const pointKeyRaw = String(
  photoPoint?.key ||
  photoPoint?.title ||
  photoPoint?.address ||
  order?.pickupPointTitle ||
  order?.pickupPointAddress ||
  order?.methodLabel ||
  ""
).trim();

const pointKeyNormalized = normalizePhotoLookupText(pointKeyRaw);

let clientPhotoSource = "";

if (String(order?.deliveryType || "").trim().toLowerCase() === "delivery") {
  const deliveryMethodNorm = normalizePhotoLookupText(order?.deliveryMethod);

  if (deliveryMethodNorm.includes("courier")) {
    clientOrderPhotoUrl = firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_COURIER,
      process.env.TG_ORDER_PHOTO_COURIER,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
    clientPhotoSource = "courier";
  } else if (deliveryMethodNorm.includes("inpost")) {
    clientOrderPhotoUrl = firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_INPOST,
      process.env.TG_ORDER_PHOTO_INPOST,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
    clientPhotoSource = "inpost";
  }
}

if (!clientOrderPhotoUrl) {
  if (pointKeyNormalized.includes("praga")) {
    clientOrderPhotoUrl = firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_PRAGA,
      process.env.TG_ORDER_PHOTO_PRAGA,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
    clientPhotoSource = "praga";
  } else if (pointKeyNormalized.includes("mokotow")) {
    clientOrderPhotoUrl = firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_MOKOTOW,
      process.env.TG_ORDER_PHOTO_MOKOTOW,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
    clientPhotoSource = "mokotow";
  } else if (pointKeyNormalized.includes("wola")) {
    clientOrderPhotoUrl = firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_WOLA,
      process.env.TG_ORDER_PHOTO_WOLA,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
    clientPhotoSource = "wola";
  } else if (pointKeyNormalized.includes("srodmiescie")) {
    clientOrderPhotoUrl = firstNonEmptyString(
      process.env.TG_CLIENT_ORDER_PHOTO_SRODMIESCIE,
      process.env.TG_ORDER_PHOTO_SRODMIESCIE,
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );
    clientPhotoSource = "srodmiescie";
  }
}

if (!clientOrderPhotoUrl) {
  clientOrderPhotoUrl = firstNonEmptyString(
    process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
    process.env.TG_ORDER_PHOTO_DEFAULT,
    managerOrderPhotoUrl
  );
  clientPhotoSource = clientPhotoSource || "default";
}

console.log("[order-photo-select]", {
  orderNo: String(order?.orderNo || ""),
  deliveryType: String(order?.deliveryType || ""),
  deliveryMethod: String(order?.deliveryMethod || ""),
  pickupPointId: String(order?.pickupPointId || ""),
  pointKey: String(point?.key || ""),
  pointTitle: String(point?.title || ""),
  pointAddress: String(point?.address || ""),
  pickupPointKey: String(pickupPoint?.key || ""),
  pickupPointTitle: String(pickupPoint?.title || ""),
  pickupPointAddress: String(pickupPoint?.address || ""),
  pointKeyRaw,
  pointKeyNormalized,
  managerOrderPhotoUrl,
  clientOrderPhotoUrl,
  clientPhotoSource,
});

const sent = await bot.telegram.sendMessage(point.notificationChatId, text, {
  parse_mode: "HTML",
  disable_web_page_preview: true,
  reply_markup: initialReplyMarkup,
});

    await Order.updateOne(
      { _id: order._id },
      {
        $set: {
          "payment.managerMessageChatId": String(point.notificationChatId || ""),
          "payment.managerMessageId": String(sent?.message_id || ""),
        },
      }
    );

    const safeTelegramId = String(order?.userTelegramId || "").trim();

    if (safeTelegramId) {
      const clientLines = [
        `🛒 <b>ЗАКАЗ СОЗДАН</b>`,
        ``,
        `🔢 <b>Номер заказа:</b> #${escapeHtml(order.orderNo)}`,
        `💰 <b>Сумма:</b> ${managerAmountText}`,
        ``,
        `ℹ️ <b>Важно:</b> менеджер получит информацию о вашем заказе только после оплаты.`,
        // ``,
        // `💵 Вы также можете выбрать способ оплаты <b>Наличные</b> и оплатить заказ на месте — в этом случае менеджер тоже получит уведомление и начнёт готовить заказ.`,
        ``,
        `👉 Откройте страницу заказа, чтобы выбрать способ оплаты и отправить его на проверку менеджеру.`,
      ];

      const clientText = clientLines.join("\n");
      const clientReplyMarkup = {
        inline_keyboard: [[{ text: "💳 Перейти к оплате", web_app: { url: `${APP_URL}/cart?orderId=${encodeURIComponent(String(order?._id || ""))}` } }]],
      };

if (clientOrderPhotoUrl) {
  try {
    await bot.telegram.sendPhoto(
      safeTelegramId,
      { url: clientOrderPhotoUrl },
      {
        caption: clientText,
        parse_mode: "HTML",
        reply_markup: clientReplyMarkup,
      }
    );
  } catch (clientPhotoErr) {
    console.error("sendOrderCreatedNotification client photo send failed:", {
      orderNo: String(order?.orderNo || ""),
      safeTelegramId,
      clientOrderPhotoUrl,
      error:
        clientPhotoErr?.response?.description ||
        clientPhotoErr?.message ||
        String(clientPhotoErr),
    });

    await bot.telegram.sendMessage(safeTelegramId, clientText, {
      parse_mode: "HTML",
      disable_web_page_preview: true,
      reply_markup: clientReplyMarkup,
    });
  }
} else {
  await bot.telegram.sendMessage(safeTelegramId, clientText, {
    parse_mode: "HTML",
    disable_web_page_preview: true,
    reply_markup: clientReplyMarkup,
  });
}
    }
  } catch (e) {
    console.error("sendOrderCreatedNotification error:", e);
  }
}

setInterval(() => {
  processDailyPointStats().catch((e) => {
    console.error("daily point stats interval error:", e);
  });
}, 60 * 1000);

processDailyPointStats().catch((e) => {
  console.error("daily point stats initial run error:", e);
});

async function refreshManagerOrderMessage(order) {
  try {
    if (!bot || !order) return { ok: false, reason: "NO_BOT_OR_ORDER" };
    

    const point = await resolveOrderNotificationPoint(order);
    const chatId = String(order?.payment?.managerMessageChatId || point?.notificationChatId || "").trim();
    const messageChatId = String(
      order?.payment?.managerMessageChatId ||
      order?.managerMessageChatId ||
      order?.managerChannelChatId ||
      order?.notificationChatId ||
      order?.managerNotificationChatId ||
      order?.managerChatId ||
      ""
    ).trim();

    const messageId = Number(
      order?.payment?.managerMessageId ||
      order?.managerMessageId ||
      order?.managerChannelMessageId ||
      order?.notificationMessageId ||
      order?.managerNotificationMessageId ||
      order?.managerMsgId ||
      0
    );

    if (!chatId || !messageId) {
      return { ok: false, reason: "NO_MANAGER_MESSAGE" };
    }

    const user = await User.findOne(
      { telegramId: String(order.userTelegramId || "") },
      { telegramId: 1, username: 1, firstName: 1 }
    ).lean();

    const customerName =
      (user?.username ? `@${user.username}` : "") ||
      String(user?.firstName || "").trim() ||
      "—";

    const itemsText = (order.items || [])
      .map((it) => {
        const productTitle =
          [it.productTitle1, it.productTitle2].filter(Boolean).join(" ").trim() ||
          it.productKey ||
          "Товар";

        const flavorsText = (it.flavors || [])
          .map((f) => {
            const flavor = f.flavorLabel || f.flavorKey || "Вкус";
            const priceText = Number(f.unitPrice || 0) > 0
              ? ` • ${Number(f.unitPrice || 0)} zł/шт.`
              : "";
            return `• ${escapeHtml(flavor)} — ${Number(f.qty || 0)} шт.${priceText}`;
          })
          .join("\n");

        return `📦 <b>${escapeHtml(productTitle)}</b>\n${flavorsText}`;
      })
      .join("\n\n");

    const paymentMethodLabel =
      order?.payment?.method === "blik"
        ? "BLIK"
        : order?.payment?.method === "crypto"
        ? "Криптовалюта"
        : order?.payment?.method === "ua_card"
        ? "Украинская карта"
        : order?.payment?.method === "cash"
        ? "Наличные"
        : "—";

    const referralFirstOrderDiscountAppliedZl = Number(order?.payment?.referralFirstOrderDiscountTotalZl || 0);
    const referralFirstOrderDiscountPercent = Number(order?.payment?.referralFirstOrderDiscountPercent || 0);
    const referralUsedCode = String(order?.payment?.referralUsedCode || "").trim();
    const hasReferralFirstOrderDiscount = order?.payment?.referralFirstOrderDiscountApplied === true;

    let inviterLabel = "";
    if (referralUsedCode) {
      const inviterUser = await User.findOne(
        { "referral.code": referralUsedCode },
        { username: 1, firstName: 1, telegramId: 1 }
      ).lean();

      inviterLabel = inviterUser?.username
        ? `@${String(inviterUser.username).trim()}`
        : String(inviterUser?.firstName || "").trim() || String(inviterUser?.telegramId || "").trim();
    }

    const managerAmountValue = Number(order?.payment?.managerDisplayAmount || 0);
    const managerAmountCurrency = String(order?.payment?.managerDisplayCurrency || "").trim();

    const managerAmountText =
      managerAmountValue > 0 && managerAmountCurrency
        ? `${managerAmountValue.toFixed(2)} ${escapeHtml(managerAmountCurrency)}`
        : `${Number(order.totalZl || 0)} ${escapeHtml(order.currency || "PLN")}`;

    const orderStatusKey = String(order?.status || "").trim().toLowerCase();
    const canceledByTelegramId = String(order?.canceledByTelegramId || "").trim();
    const userTelegramId = String(order?.userTelegramId || "").trim();

    const canceledByClient =
      orderStatusKey === "canceled" &&
      canceledByTelegramId &&
      canceledByTelegramId === userTelegramId;

    const paymentStatusKey = String(order?.payment?.status || "").trim().toLowerCase();

    const paymentStatusLabel =
      orderStatusKey === "annulled"
        ? "⌛️ Аннулирован из-за отсутствия подтверждения оплаты"
        : orderStatusKey === "canceled"
        ? canceledByClient
          ? "❌ Отменен клиентом"
          : "❌ Отклонен менеджером"
          : paymentStatusKey === "paid"
          ? "✅ Оплачено"
          : paymentStatusKey === "awaiting"
          ? "🕒 Ожидаю клиента"
          : paymentStatusKey === "checking"
          ? "🟠 Оплата на проверке"
          : "❌ Не оплачено";

    const orderStatusLabel =
      orderStatusKey === "completed"
        ? String(order?.deliveryType || "") === "delivery" && String(order?.deliveryMethod || "") === "courier"
          ? "🚚 Доставлен"
          : "✅ Выполнен"
        : orderStatusKey === "shipped"
        ? "📦 Отправлен"
        : orderStatusKey === "annulled"
        ? "⌛️ Аннулирован"
        : orderStatusKey === "canceled"
        ? canceledByClient
          ? "❌ Отменен клиентом"
          : "❌ Отклонен менеджером"
        : orderStatusKey === "assembled"
        ? "🟠 Заказ собран"
        : orderStatusKey === "processing"
        ? "🟠 В процессе"
        : "⚪️ Создан";

    const lines = [
      `🛒 <b>ОПЛАТА ОТПРАВЛЕНА НА ПРОВЕРКУ</b>`,
      ``,
      `🔢 <b>Номер:</b> #${escapeHtml(order.orderNo)}`,
      ``,
      `👤 <b>Клиент:</b> ${escapeHtml(customerName)}`,
      `🕒 <b>Создан:</b> ${escapeHtml(formatOrderDate(order.createdAt))}`,
      ``,
      `📋 <b>Состав заказа:</b>`,
      ``,
      itemsText || "—",
      ``,
      `💰 <b>Сумма заказа:</b> ${managerAmountText}`,
      `💳 <b>Способ оплаты:</b> ${
        order?.payment?.cashbackFullyPaid
          ? "Кэшбек"
          : escapeHtml(paymentMethodLabel)
      }`,
      ``,
      order?.payment?.cashbackAppliedZl > 0
        ? `🪙 <b>Оплачено кэшбеком:</b> ${Number(order.payment.cashbackAppliedZl || 0).toFixed(2)} ${escapeHtml(order.currency || "PLN")}`
        : null,
      order?.payment?.cashbackAppliedZl > 0 && Number(order?.payment?.cashbackRemainingToPayZl || 0) > 0
        ? `💸 <b>Остаток к оплате:</b> ${Number(order.payment.cashbackRemainingToPayZl || 0).toFixed(2)} ${escapeHtml(order.currency || "PLN")}`
        : null,
      hasReferralFirstOrderDiscount
        ? `🎁 <b>Реферальная скидка:</b> ${referralFirstOrderDiscountPercent || 10}% на первый заказ${referralFirstOrderDiscountAppliedZl > 0 ? ` (${referralFirstOrderDiscountAppliedZl.toFixed(2)} PLN)` : ""}${inviterLabel ? `\n👤 <b>Пригласитель:</b> ${escapeHtml(inviterLabel)}` : ""}`
        : null,
      ``,
      orderStatusKey === "annulled"
        ? `💳 <b>Статус оплаты:</b> ⌛️ Аннулирован из-за отсутствия подтверждения оплаты`
        : order?.payment?.cashbackFullyPaid
        ? `💳 <b>Статус оплаты:</b> ✅ Полностью оплачено`
        : String(order?.payment?.status || "") === "paid"
        ? `💳 <b>Статус оплаты:</b> ✅ Оплачено`
        : String(order?.payment?.status || "") === "awaiting"
        ? `💳 <b>Статус оплаты:</b> 🕒 Ожидаю клиента`
        : String(order?.payment?.status || "") === "checking"
        ? `💳 <b>Статус оплаты:</b> 🟠 Оплата на проверке`
        : `💳 <b>Статус оплаты:</b> ❌ Не оплачено`,
      `📦 <b>Статус заказа:</b> ${orderStatusLabel}`,
      ``,
    ];

    if (order.deliveryType === "pickup" && order.arrivalTime) {
      lines.push(`🚚 <b>Клиент будет в ${escapeHtml(order.arrivalTime)}</b>`);
      lines.push("");
    }

if (order.deliveryType === "delivery" && order.deliveryMethod === "courier") {
  if (order.courierAddress) {
    lines.push(`📍 <b>Адрес доставки:</b> ${escapeHtml(order.courierAddress)}`);
  }
  if (order.courierDistrict) {
    lines.push(`🌍 <b>Район:</b> ${escapeHtml(order.courierDistrict)}`);
  }
  if (Number(order.deliveryFeeZl || 0) > 0) {
    lines.push(`🚚 <b>Стоимость доставки:</b> ${Number(order.deliveryFeeZl || 0).toFixed(2)} PLN`);
  }
  if (order.deliveryTimeWindow) {
    lines.push(`🕒 <b>Временной промежуток:</b> ${escapeHtml(order.deliveryTimeWindow)}`);
  }
  lines.push("");
}

    if (order.deliveryType === "delivery" && order.deliveryMethod === "inpost") {
      if (order.inpostData?.fullName) lines.push(`👤 <b>Получатель:</b> ${escapeHtml(order.inpostData.fullName)}`);
      if (order.inpostData?.phone) lines.push(`📞 <b>Телефон:</b> ${escapeHtml(order.inpostData.phone)}`);
      if (order.inpostData?.email) lines.push(`✉️ <b>Email:</b> ${escapeHtml(order.inpostData.email)}`);
      if (order.inpostData?.city) lines.push(`🏙 <b>Город:</b> ${escapeHtml(order.inpostData.city)}`);
      if (order.inpostData?.lockerAddress) lines.push(`📦 <b>Пачкомат:</b> ${escapeHtml(order.inpostData.lockerAddress)}`);
      if (
        order.inpostData?.fullName ||
        order.inpostData?.phone ||
        order.inpostData?.email ||
        order.inpostData?.city ||
        order.inpostData?.lockerAddress
      ) {
        lines.push("");
      }
      if (Number(order?.inpostDeliveryFeeZl || 0) > 0) {
        lines.push(`🚚 <b>Стоимость доставки InPost:</b> ${Number(order.inpostDeliveryFeeZl || 0).toFixed(2)} PLN`);
      }
      if (Number(order?.inpostPackageUnits || 0) > 0) {
        lines.push(`📦 <b>Условные единицы:</b> ${Number(order.inpostPackageUnits || 0).toFixed(2)}`);
      }
    }

    if (order.payment?.method === "cash") {
      if (order.payment?.cashChangeType === "need_change" && order.payment?.cashAmount) {
        lines.push(`💵 <b>Сдача:</b> ${escapeHtml(order.payment.cashAmount)} zł`);
        lines.push("");
      } else if (order.payment?.cashChangeType === "no_change") {
        lines.push(`💵 <b>Сдача:</b> без сдачи`);
        lines.push("");
      }
    }

    const text = lines.filter((line) => line !== null && line !== undefined).join("\n");

const replyMarkup =
  orderStatusKey === "completed"
    ? {
        inline_keyboard: [
          [{
            text:
              String(order?.deliveryType || "") === "delivery" &&
              String(order?.deliveryMethod || "") === "courier"
                ? "🚚 Заказ доставлен"
                : "✅ Заказ выполнен",
            callback_data: `mgr_order_completed_done:${order._id}`,
          }],
        ],
      }
    : orderStatusKey === "shipped"
    ? {
        inline_keyboard: [
          [{ text: "📦 Заказ отправлен", callback_data: `mgr_order_shipped_done:${order._id}` }],
        ],
      }
    : orderStatusKey === "annulled"
    ? {
        inline_keyboard: [
          [{ text: "⌛️ Заказ аннулирован", callback_data: `mgr_order_annulled_done:${order._id}` }],
        ],
      }
    : orderStatusKey === "canceled"
    ? {
        inline_keyboard: [
          [
            {
              text: canceledByClient
                ? "❌ Заказ отменен клиентом"
                : "❌ Заказ отклонен менеджером",
              callback_data: `mgr_order_canceled_done:${order._id}`,
            },
          ],
        ],
      }
    : String(order?.payment?.status || "") === "awaiting"
    ? {
        inline_keyboard: [
          [{ text: "🕒 Ожидаю", callback_data: `mgr_done:${order._id}` }],
        ],
      }
    : String(order?.deliveryType || "") === "pickup" &&
      String(order?.payment?.method || "") === "cash" &&
      String(order?.payment?.status || "") !== "awaiting"
    ? {
        inline_keyboard: [
          [
            { text: "🕒 Ожидаю", callback_data: `mgr_pay_paid:${order._id}` },
            { text: "❌ Отклонить", callback_data: `mgr_pay_unpaid:${order._id}` },
          ],
        ],
      }
    : String(order?.payment?.status || "") === "paid"
    ? {
        inline_keyboard: [
          [{ text: "✅ Оплачено", callback_data: `mgr_done:${order._id}` }],
        ],
      }
    : {
        inline_keyboard: [
          [
            { text: "✅ Оплачено", callback_data: `mgr_pay_paid:${order._id}` },
            { text: "❌ Отклонить", callback_data: `mgr_pay_unpaid:${order._id}` },
          ],
        ],
      };

    try {
      await bot.telegram.editMessageText(
        messageChatId,
        messageId,
        undefined,
        lines.join("\n"),
        {
          parse_mode: "HTML",
          disable_web_page_preview: true,
          reply_markup: replyMarkup,
        }
      );
    } catch (editErr) {
      const editMsg = String(editErr?.response?.description || editErr?.message || "").toLowerCase();

      if (
        editMsg.includes("there is no text in the message to edit") ||
        editMsg.includes("message content is not modified") ||
        editMsg.includes("message is not modified") ||
        editMsg.includes("message can't be edited")
      ) {
        try {
          await bot.telegram.editMessageCaption(
            messageChatId,
            messageId,
            undefined,
            lines.join("\n"),
            {
              parse_mode: "HTML",
              reply_markup: replyMarkup,
            }
          );
          return { ok: true };
        } catch (captionErr) {
          const captionMsg = String(
            captionErr?.response?.description || captionErr?.message || ""
          ).toLowerCase();

          if (
            captionMsg.includes("message is not modified") ||
            captionMsg.includes("message content is not modified")
          ) {
            try {
              await bot.telegram.editMessageReplyMarkup(
                messageChatId,
                messageId,
                undefined,
                replyMarkup
              );
            } catch (markupErr) {
              const markupMsg = String(
                markupErr?.response?.description || markupErr?.message || ""
              ).toLowerCase();

              if (!markupMsg.includes("message is not modified")) {
                console.error("refreshManagerOrderMessage editMessageReplyMarkup error:", markupErr);
              }
            }
            return { ok: true };
          }

          console.error("refreshManagerOrderMessage editMessageCaption error:", captionErr);
        }
      } else {
        console.error("refreshManagerOrderMessage editMessageText error:", editErr);
      }

      try {
        await bot.telegram.editMessageReplyMarkup(
          messageChatId,
          messageId,
          undefined,
          replyMarkup
        );
      } catch (e) {
        const msg = String(e?.response?.description || e?.message || "").toLowerCase();

        if (!msg.includes("message is not modified")) {
          console.error("refreshManagerOrderMessage editMessageReplyMarkup error:", e);
        }
      }
    }

    return { ok: true };
  } catch (e) {
    const msg = String(e?.response?.description || e?.message || "").toLowerCase();

    if (msg.includes("message is not modified")) {
      return { ok: true };
    }

    console.error("refreshManagerOrderMessage error:", e);
    return { ok: false, reason: "EDIT_FAILED" };
  }
}

async function sendClientOrderCreatedInfo(order) {
  try {
    if (!bot || !order?.userTelegramId) return;

    const webAppBaseUrl = String(process.env.WEBAPP_URL || "")
      .trim()
      .replace(/\/$/, "");

    const orderNo = String(order.orderNo || "").trim();
    const orderLink = webAppBaseUrl ? `${webAppBaseUrl}/orders` : null;

    const point = await resolveOrderNotificationPoint(order).catch(() => null);
    const pickupPoint = order?.pickupPointId
      ? await PickupPoint.findById(order.pickupPointId).lean().catch(() => null)
      : null;

    const photoPoint = point || pickupPoint || null;

    const photoUrl = firstNonEmptyString(
      getCustomerOrderPhotoByPickupPoint(order, photoPoint),
      getManagerOrderPhotoByPickupPoint(order, photoPoint),
      process.env.TG_CLIENT_ORDER_PHOTO_DEFAULT,
      process.env.TG_ORDER_PHOTO_DEFAULT
    );

    const lines = [
      `🛒 <b>ЗАКАЗ СОЗДАН</b>`,
      ``,
      `🔢 <b>Номер заказа:</b> #${escapeHtml(orderNo)}`,
      `💰 <b>Сумма:</b> ${Number(order.totalZl || 0)} ${escapeHtml(order.currency || "PLN")}`,
      ``,
      `ℹ️ <b>Важно:</b> менеджер получит информацию о вашем заказе <b>только после оплаты</b>.`,
      // ``,
      // `💵 Вы также можете выбрать способ оплаты <b>Наличные</b> и оплатить заказ на месте — в этом случае менеджер тоже получит уведомление и начнёт готовить заказ.`,
      ``,
      `👉 Откройте страницу заказа, чтобы выбрать способ оплаты и отправить его на проверку менеджеру.`,
    ];

    const extra = {
      parse_mode: "HTML",
      disable_web_page_preview: true,
    };

    if (orderLink) {
      extra.reply_markup = {
        inline_keyboard: [
          [{ text: "💳 Перейти к оплате", web_app: { url: orderLink } }],
        ],
      };
    }

    if (photoUrl) {
      await bot.telegram.sendPhoto(
        String(order.userTelegramId),
        { url: photoUrl },
        {
          caption: lines.join("\n"),
          ...extra,
        }
      );
      return;
    }

    await bot.telegram.sendMessage(
      String(order.userTelegramId),
      lines.join("\n"),
      extra
    );
  } catch (e) {
    const errorCode = Number(e?.response?.error_code || 0);
    const description = String(
      e?.response?.description || e?.description || e?.message || ""
    );

    if (
      (errorCode === 403 && /bot was blocked by the user/i.test(description)) ||
      (errorCode === 400 && /chat not found/i.test(description))
    ) {
      console.warn("sendClientOrderCreatedInfo skipped: user chat is unavailable", {
        orderNo: order?.orderNo,
        telegramId: String(order?.userTelegramId || ""),
        errorCode,
        description,
      });
      return;
    }

    console.error("sendClientOrderCreatedInfo error:", e);
  }
}

const ORDER_PAYMENT_REMINDER_START_DELAY_MS = 5 * 60 * 1000; // 5 минут
const ORDER_PAYMENT_REMINDER_INTERVAL_MS = 35 * 1000; // 35 секунд

const paymentReminderTimeouts = new Map(); // orderId -> timeoutId
const paymentReminderIntervals = new Map(); // orderId -> intervalId

function stopPaymentReminder(orderId) {
  const key = String(orderId || "").trim();
  if (!key) return;

  const timeoutId = paymentReminderTimeouts.get(key);
  if (timeoutId) {
    clearTimeout(timeoutId);
    paymentReminderTimeouts.delete(key);
  }

  const intervalId = paymentReminderIntervals.get(key);
  if (intervalId) {
    clearInterval(intervalId);
    paymentReminderIntervals.delete(key);
  }
}

async function startPaymentReminder(order) {
  try {
    if (!bot || !order?._id || !order?.userTelegramId) return;

    const orderId = String(order._id || "").trim();
    if (!orderId) return;

    // если вдруг уже есть таймер по этому заказу — пересоздаём
    stopPaymentReminder(orderId);

    const tick = async () => {
      try {
        const fresh = await Order.findById(orderId, {
          _id: 1,
          orderNo: 1,
          userTelegramId: 1,
          totalZl: 1,
          currency: 1,
          status: 1,
          payment: 1,
        }).lean();

        if (!fresh) {
          stopPaymentReminder(orderId);
          return;
        }

        const paymentStatus = String(fresh?.payment?.status || "unpaid");
        const orderStatus = String(fresh?.status || "").toLowerCase();

        // как только заказ ушёл на проверку / подтверждён / отменён / аннулирован — останавливаем напоминания
        if (
          ["checking", "paid", "refunded"].includes(paymentStatus) ||
          ["canceled", "annulled", "completed", "done", "shipped"].includes(orderStatus)
        ) {
          stopPaymentReminder(orderId);
          return;
        }

        const webAppBaseUrl = String(process.env.WEB_APP_URL || process.env.WEBAPP_URL || "")
          .trim()
          .replace(/\/$/, "");

        const orderLink =
          /^https:\/\/.+/i.test(webAppBaseUrl)
            ? `${webAppBaseUrl}/orders`
            : null;

        const lines = [
          `⏰ <b>НАПОМИНАНИЕ ОБ ОПЛАТЕ</b>`,
          ``,
          `🔢 <b>Номер заказа:</b> #${escapeHtml(fresh.orderNo)}`,
          `💰 <b>Сумма:</b> ${Number(fresh.totalZl || 0)} ${escapeHtml(fresh.currency || "PLN")}`,
          ``,
          `Менеджер получит информацию о вашем заказе <b>только после оплаты</b>.`,
          ``,
          `💵 Если вам удобнее, выберите способ оплаты <b>Наличные</b> — тогда менеджер тоже получит уведомление и начнёт готовить заказ.`,
        ];

        const extra = {
          parse_mode: "HTML",
          disable_web_page_preview: true,
        };

        if (orderLink) {
          extra.reply_markup = {
            inline_keyboard: [
              [{ text: "💳 Перейти к оплате", web_app: { url: orderLink } }],
            ],
          };
        }

        await bot.telegram.sendMessage(
          String(fresh.userTelegramId),
          lines.join("\n"),
          extra
        );
      } catch (e) {
        console.error("payment reminder tick error:", e);
      }
    };

    const startTimeoutId = setTimeout(() => {
      tick();

      const intervalId = setInterval(() => {
        tick();
      }, ORDER_PAYMENT_REMINDER_INTERVAL_MS);

      paymentReminderIntervals.set(orderId, intervalId);
    }, ORDER_PAYMENT_REMINDER_START_DELAY_MS);

    paymentReminderTimeouts.set(orderId, startTimeoutId);
  } catch (e) {
    console.error("startPaymentReminder error:", e);
  }
}

async function resolveOrderReservePickupPointId(order) {
  if (!order) return null;

  if (order.deliveryType === "pickup" && order.pickupPointId) {
    return order.pickupPointId;
  }

  if (order.deliveryType === "delivery") {
    const deliveryKey = order.deliveryMethod === "inpost" ? "delivery-2" : "delivery";
    const point = await PickupPoint.findOne(
      { key: { $in: [deliveryKey, `${deliveryKey},`] } },
      { _id: 1 }
    ).lean();

    return point?._id || null;
  }

  return null;
}

async function releaseOrderReservedStock(order) {
  if (!order || order.stockReleasedAt) return false;

  const pickupPointId = await resolveOrderReservePickupPointId(order);
  if (!pickupPointId) return false;

  const pointObjId =
    pickupPointId instanceof mongoose.Types.ObjectId
      ? pickupPointId
      : new mongoose.Types.ObjectId(String(pickupPointId));

  const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

  for (const item of order.items || []) {
    const productKey = String(item.productKey || "").trim();
    if (!productKey) continue;

    for (const fl of item.flavors || []) {
      const qty = Math.max(0, Number(fl.qty || 0));
      if (!qty) continue;

      const fkNorm = normFlavorKey(fl.flavorKey);
      const fkCandidates = Array.from(
        new Set([String(fl.flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean))
      );

      // 1) уменьшаем reservedQty
      await Product.updateOne(
        {
          productKey,
          "flavors.flavorKey": { $in: fkCandidates },
          "flavors.stockByPickupPoint.pickupPointId": pointObjId,
        },
        {
          $inc: {
            "flavors.$[f].stockByPickupPoint.$[s].reservedQty": -qty,
          },
        },
        {
          arrayFilters: [
            { "f.flavorKey": { $in: fkCandidates } },
            { "s.pickupPointId": pointObjId },
          ],
        }
      );

      // 2) clamp: reservedQty не должен быть < 0
      await Product.updateOne(
        { productKey },
        [
          {
            $set: {
              flavors: {
                $map: {
                  input: "$flavors",
                  as: "f",
                  in: {
                    $cond: [
                      { $in: ["$$f.flavorKey", fkCandidates] },
                      {
                        $mergeObjects: [
                          "$$f",
                          {
                            stockByPickupPoint: {
                              $map: {
                                input: "$$f.stockByPickupPoint",
                                as: "s",
                                in: {
                                  $cond: [
                                    { $eq: ["$$s.pickupPointId", pointObjId] },
                                    {
                                      $mergeObjects: [
                                        "$$s",
                                        {
                                          reservedQty: {
                                            $max: [0, { $ifNull: ["$$s.reservedQty", 0] }],
                                          },
                                        },
                                      ],
                                    },
                                    "$$s",
                                  ],
                                },
                              },
                            },
                          },
                        ],
                      },
                      "$$f",
                    ],
                  },
                },
              },
            },
          },
        ]
      );
    }
  }

  return true;
}

async function commitOrderStock(order) {
  if (!order || order.stockCommittedAt) return false;

  const pickupPointId = await resolveOrderReservePickupPointId(order);
  if (!pickupPointId) return false;

  const pointObjId =
    pickupPointId instanceof mongoose.Types.ObjectId
      ? pickupPointId
      : new mongoose.Types.ObjectId(String(pickupPointId));

  const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

  for (const item of order.items || []) {
    const productKey = String(item.productKey || "").trim();
    if (!productKey) continue;

    for (const fl of item.flavors || []) {
      const qty = Math.max(0, Number(fl.qty || 0));
      if (!qty) continue;

      const fkNorm = normFlavorKey(fl.flavorKey);
      const fkCandidates = Array.from(
        new Set([String(fl.flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean))
      );

      // 1) totalQty -= qty, reservedQty -= qty
      await Product.updateOne(
        {
          productKey,
          "flavors.flavorKey": { $in: fkCandidates },
          "flavors.stockByPickupPoint.pickupPointId": pointObjId,
        },
        {
          $inc: {
            "flavors.$[f].stockByPickupPoint.$[s].totalQty": -qty,
            "flavors.$[f].stockByPickupPoint.$[s].reservedQty": -qty,
          },
        },
        {
          arrayFilters: [
            { "f.flavorKey": { $in: fkCandidates } },
            { "s.pickupPointId": pointObjId },
          ],
        }
      );

      // 2) clamp: totalQty/reservedQty не должны быть < 0,
      //    reservedQty не должен быть > totalQty
      await Product.updateOne(
        { productKey },
        [
          {
            $set: {
              flavors: {
                $map: {
                  input: "$flavors",
                  as: "f",
                  in: {
                    $cond: [
                      { $in: ["$$f.flavorKey", fkCandidates] },
                      {
                        $mergeObjects: [
                          "$$f",
                          {
                            stockByPickupPoint: {
                              $map: {
                                input: "$$f.stockByPickupPoint",
                                as: "s",
                                in: {
                                  $cond: [
                                    { $eq: ["$$s.pickupPointId", pointObjId] },
                                    {
                                      $let: {
                                        vars: {
                                          safeTotal: {
                                            $max: [0, { $ifNull: ["$$s.totalQty", 0] }],
                                          },
                                          safeReservedRaw: {
                                            $max: [0, { $ifNull: ["$$s.reservedQty", 0] }],
                                          },
                                        },
                                        in: {
                                          $mergeObjects: [
                                            "$$s",
                                            {
                                              totalQty: "$$safeTotal",
                                              reservedQty: {
                                                $min: ["$$safeReservedRaw", "$$safeTotal"],
                                              },
                                            },
                                          ],
                                        },
                                      },
                                    },
                                    "$$s",
                                  ],
                                },
                              },
                            },
                          },
                        ],
                      },
                      "$$f",
                    ],
                  },
                },
              },
            },
          },
        ]
      );
    }
  }

  return true;
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

function slugifyFlavorLabel(input) {
  const base = translitRuToLat(input)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 32);

  return base || "flavor";
}

function ensureUniqueFlavorKeyForProduct(product, baseKey) {
  const existingKeys = new Set(
    (product?.flavors || [])
      .map((f) => String(f?.flavorKey || "").trim().toLowerCase())
      .filter(Boolean)
  );

  const cleanBase =
    String(baseKey || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 32) || "flavor";

  if (!existingKeys.has(cleanBase)) return cleanBase;

  for (let i = 2; i <= 999; i++) {
    const suffix = `-${i}`;
    const cut = Math.max(1, 32 - suffix.length);
    const candidate = `${cleanBase.slice(0, cut).replace(/-+$/g, "")}${suffix}`;
    if (!existingKeys.has(candidate)) return candidate;
  }

  return `${cleanBase.slice(0, 24).replace(/-+$/g, "")}-${Date.now().toString(36).slice(-6)}`;
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

async function attachReferralIfAny(user, normalizedRef) {
  const safeRef = String(normalizedRef || "").replace(/^ref_/, "").trim();
  if (!user || !safeRef) return false;

  user.referral = user.referral || {};

  if (String(user.referral.usedCode || "").trim()) {
    return false;
  }

  let inviter = await User.findOne({ "referral.code": safeRef });
  if (!inviter && /^\d+$/.test(safeRef)) {
    inviter = await User.findOne({ telegramId: safeRef });
  }
  if (!inviter) return false;

  if (String(inviter.telegramId || "") === String(user.telegramId || "")) {
    return false;
  }

  user.referral.usedCode = safeRef;
  user.referral.invitedByTelegramId = String(inviter.telegramId || "");

  const addedToGroup = attachReferralToRewardGroup(inviter, user.telegramId);
  if (addedToGroup) {
    if (typeof inviter.markModified === "function") {
      inviter.markModified("referral.rewardGroups");
    }
    await inviter.save();
  }

  await user.save();
  return true;
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
    const { telegramId, username, firstName, lastName, photoUrl, ref } = req.body || {};
    const normalizedRef = String(ref || "").replace(/^ref_/, "").trim();
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    let user = await User.findOne({ telegramId: String(telegramId) });

    if (!user) {
      const newUser = await User.create({
        telegramId: String(telegramId),
        username: username || null,
        firstName: firstName || null,
        lastName: lastName || null,
        photoUrl: photoUrl || null,
      });

      await ensureUserRefCode(newUser);
      await attachReferralIfAny(newUser, normalizedRef);

      const fresh = await User.findById(newUser._id).lean();
      return res.json({ ok: true, user: fresh });
    }

    user.username = username || user.username;
    user.firstName = firstName || user.firstName;
    user.lastName = lastName || user.lastName;
    user.photoUrl = photoUrl || user.photoUrl;
    await user.save();

    if (normalizedRef) {
      await attachReferralIfAny(user, normalizedRef);
      user = await User.findById(user._id);
    }

    await ensureUserRefCode(user);

    return res.json({ ok: true, user });
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

app.get("/cart/summary", async (req, res) => {
  try {
    const telegramId = String(req.query?.telegramId || "").trim();
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const cart = await Cart.findOne({ telegramId }).lean();
    const items = Array.isArray(cart?.items) ? cart.items : [];

    const totalZl = Number(
      items
        .reduce(
          (sum, item) =>
            sum + Number(item?.qty || 0) * Number(item?.unitPrice || 0),
          0
        )
        .toFixed(2)
    );

    return res.json({
      ok: true,
      totalZl,
      itemsCount: items.length,
    });
  } catch (e) {
    console.error("GET /cart/summary error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.get("/referral/status", async (req, res) => {
  try {
    const telegramId = String(req.query.telegramId || "").trim();
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const user = await User.findOne(
      { telegramId },
      { telegramId: 1, referral: 1 }
    );

    if (!user) {
      return res.status(404).json({ ok: false, error: "User not found" });
    }

    const referralStatus = await buildReferralStatusForUser(user);

    return res.json({
      ok: true,
      referralStatus,
    });
  } catch (e) {
    console.error("GET /referral/status error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.post("/referral/claim", async (req, res) => {
  try {
    const { telegramId, groupId } = req.body || {};
    const safeTelegramId = String(telegramId || "").trim();
    const safeGroupId = String(groupId || "").trim();

    if (!safeTelegramId) {
      return res.status(400).json({ ok: false, error: "TELEGRAM_ID_REQUIRED" });
    }

    if (!safeGroupId) {
      return res.status(400).json({ ok: false, error: "GROUP_ID_REQUIRED" });
    }

    const user = await User.findOne({ telegramId: safeTelegramId });
    if (!user) {
      return res.status(404).json({ ok: false, error: "USER_NOT_FOUND" });
    }

    const referralStatus = await buildReferralStatusForUser(user);
    const groups = Array.isArray(referralStatus?.groups) ? referralStatus.groups : [];

    const selectedGroup = groups.find(
      (group) => String(group?.id || "") === safeGroupId
    );

    if (!selectedGroup) {
      return res.status(404).json({ ok: false, error: "REFERRAL_GROUP_NOT_FOUND" });
    }

    if (selectedGroup.rewardClaimed === true || selectedGroup.isClaimed === true) {
      return res.json({ ok: false, status: "ALREADY_CLAIMED" });
    }

    const memberIds = Array.isArray(selectedGroup?.members)
      ? selectedGroup.members.map((m) => String(m?.telegramId || "").trim()).filter(Boolean)
      : [];

    if (memberIds.length < 2) {
      return res.json({ ok: false, status: "NOT_ENOUGH_REFERRALS" });
    }

    const referredUsers = await User.find(
      { telegramId: { $in: memberIds } },
      { telegramId: 1, username: 1, firstName: 1, referral: 1 }
    ).lean();

    const referredById = new Map(
      referredUsers.map((row) => [String(row.telegramId || ""), row])
    );

    const paidOrderTelegramIds = await Order.distinct("userTelegramId", {
      userTelegramId: { $in: memberIds },
      $or: [
        { "payment.status": "paid" },
        { status: { $in: ["processing", "done"] } },
      ],
    });

    const paidSet = new Set(
      (paidOrderTelegramIds || []).map((x) => String(x || "").trim())
    );

    const members = memberIds.map((tgId) => {
      const refUser = referredById.get(tgId);
      const completed =
        Boolean(refUser?.referral?.firstOrderDoneAt) || paidSet.has(tgId);

      return {
        telegramId: tgId,
        displayName: getReferralDisplayName(refUser || { telegramId: tgId }),
        completed,
      };
    });

    const completedMembers = members.filter((m) => m?.completed === true);
    const pendingMembers = members.filter((m) => m?.completed !== true);

    if (completedMembers.length === 1 && pendingMembers.length === 1) {
      return res.json({
        ok: false,
        status: "ONE_COMPLETED",
        completed: completedMembers[0].displayName,
        pending: pendingMembers[0].displayName,
      });
    }

    if (completedMembers.length === 0) {
      return res.json({
        ok: false,
        status: "NONE_COMPLETED",
        users: members.map((m) => m.displayName),
      });
    }

    if (completedMembers.length !== 2) {
      return res.json({ ok: false, status: "GROUP_NOT_READY" });
    }

    const realGroups = ensureReferralGroupsArray(user);
    const targetGroup = realGroups.find(
      (group) => String(group?._id || "") === safeGroupId
    );

    if (!targetGroup) {
      return res.status(404).json({ ok: false, error: "REFERRAL_GROUP_NOT_FOUND" });
    }

    if (targetGroup.rewardClaimed === true) {
      return res.json({ ok: false, status: "ALREADY_CLAIMED" });
    }

    targetGroup.rewardClaimed = true;
    targetGroup.rewardClaimedAt = new Date();

    user.cashbackLedger = Array.isArray(user.cashbackLedger) ? user.cashbackLedger : [];
    user.cashbackLedger.push({
      sourceOrderId: null,
      amountZl: 25,
      remainingZl: 25,
      earnedAt: new Date(),
      expiresAt: addDays(new Date(), 40),
      warnedAt: null,
      expiredAt: null,
    });

    if (typeof user.markModified === "function") {
      user.markModified("referral.rewardGroups");
      user.markModified("cashbackLedger");
    }

    recalcUserCashbackBalanceFromLedger(user);
    await user.save();

    return res.json({
      ok: true,
      status: "REWARD_GRANTED",
      amount: 25,
      groupId: safeGroupId,
    });
  } catch (e) {
    console.error("POST /referral/claim error:", e);
    res.status(500).json({ ok: false, error: "SERVER_ERROR" });
  }
});

// ==== Public: get favorites by telegramId ====
app.get("/favorites", async (req, res) => {
  try {
    const telegramId = String(req.query.telegramId || "").trim();
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const user = await User.findOne({ telegramId }, { favoriteProductKeys: 1 }).lean();

    return res.json({
      ok: true,
      favoriteProductKeys: Array.isArray(user?.favoriteProductKeys)
        ? user.favoriteProductKeys
        : [],
    });
  } catch (e) {
    console.error("GET /favorites error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: toggle favorite product =====
app.post("/favorites/toggle", async (req, res) => {
  try {
    const telegramId = String(req.body?.telegramId || "").trim();
    const productKey = String(req.body?.productKey || "").trim();

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    if (!productKey) {
      return res.status(400).json({ ok: false, error: "productKey is required" });
    }

    const user = await User.findOne({ telegramId });
    if (!user) {
      return res.status(404).json({ ok: false, error: "User not found" });
    }

    const current = Array.isArray(user.favoriteProductKeys)
      ? user.favoriteProductKeys.map((x) => String(x))
      : [];

    const exists = current.includes(productKey);

    if (exists) {
      user.favoriteProductKeys = current.filter((x) => x !== productKey);
    } else {
      user.favoriteProductKeys = [...current, productKey];
    }

    await user.save();

    return res.json({
      ok: true,
      isFavorite: !exists,
      favoriteProductKeys: user.favoriteProductKeys || [],
    });
  } catch (e) {
    console.error("POST /favorites/toggle error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
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
    const photo = "https://blush-impressive-moth-462.mypinata.cloud/ipfs/bafybeifdnnsv4ddcjyorb3xrf6fvzi4yyqizuhhpomv7aew4j7oodfsg3q";

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

    const notificationChatId = String(b.notificationChatId || "").trim();

    const statsChatId = String(b.statsChatId || "").trim();
    const statsSendTime = String(b.statsSendTime || "23:59").trim();
    const scheduleByDate =
      b.scheduleByDate && typeof b.scheduleByDate === "object"
        ? b.scheduleByDate
        : {};

    const created = await PickupPoint.create({
      key: finalKey,
      title: rawTitle,
      address: rawAddress,
      sortOrder: Number(b.sortOrder || 0),
      isActive: b.isActive ?? true,
      allowedAdminTelegramIds: allowed,
      notificationChatId,
      statsChatId,
      statsSendTime,
      scheduleByDate,
    });

    res.json({ ok: true, pickupPoint: created });
  } catch (e) {
    console.error("POST /admin/pickup-points error:", e);
    if (e?.code === 11000) return res.status(409).json({ ok: false, error: "Pickup point key already exists" });
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.post("/admin/users/cashback/grant-by-username", async (req, res) => {
  try {
    const token = String(req.headers["x-admin-token"] || "").trim();
    if (!token || token !== process.env.ADMIN_API_TOKEN) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }

    const usernameRaw = String(req.body?.username || "").trim();
    const username = usernameRaw.replace(/^@+/, "").trim();
    const amountZl = Number(req.body?.amountZl || 0);
    // const note = String(req.body?.note || "").trim();
    const grantedByTelegramId = String(req.body?.grantedByTelegramId || "").trim();
    const grantedByUsername = String(req.body?.grantedByUsername || "").trim();

    if (!username) {
      return res.status(400).json({ ok: false, error: "USERNAME_REQUIRED" });
    }

    if (!(amountZl > 0)) {
      return res.status(400).json({ ok: false, error: "INVALID_CASHBACK_AMOUNT" });
    }

    const user = await User.findOne({ username });
    if (!user) {
      return res.status(404).json({ ok: false, error: "USER_NOT_FOUND" });
    }

    const result = await grantManualCashbackToUser(user, amountZl, {
      // note,
      grantedByTelegramId,
      grantedByUsername,
    });

    return res.json({
      ok: true,
      user: {
        telegramId: String(user.telegramId || ""),
        username: String(user.username || ""),
        firstName: String(user.firstName || ""),
      },
      cashbackBalance: Number(result.cashbackBalance || 0),
      grantedAmountZl: Number(result.grantedAmountZl || 0),
      expiresAt: result.expiresAt,
    });
  } catch (e) {
    console.error("POST /admin/users/cashback/grant-by-username error:", e);
    return res.status(500).json({ ok: false, error: e.message || "SERVER_ERROR" });
  }
});

app.patch("/admin/pickup-points/:id", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const b = req.body || {};

    const allow = [
      "key",
      "title",
      "address",
      "sortOrder",
      "isActive",
      "allowedAdminTelegramIds",
      "notificationChatId",
      "statsChatId",
      "statsSendTime",
      "paymentConfig",
      "scheduleByDatePatch",
      "scheduleByDate",
    ];
    
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

    if (update.notificationChatId !== undefined) {
      update.notificationChatId = String(update.notificationChatId || "").trim();
    }

    if (update.statsChatId !== undefined) {
      update.statsChatId = String(update.statsChatId || "").trim();
    }

    if (update.statsSendTime !== undefined) {
      update.statsSendTime = String(update.statsSendTime || "23:59").trim();
    }

    if (update.scheduleByDate !== undefined) {
      update.scheduleByDate =
        update.scheduleByDate && typeof update.scheduleByDate === "object"
          ? update.scheduleByDate
          : {};
    }

    if (update.paymentConfig !== undefined) {
      const rawMethods = Array.isArray(update.paymentConfig?.methods)
        ? update.paymentConfig.methods
        : [];

      update.paymentConfig = {
        methods: rawMethods
          .map((m) => ({
            key: String(m?.key || "").trim(),
            label: String(m?.label || "").trim(),
            detailsValue: String(m?.detailsValue || "").trim(),
            badge: String(m?.badge || "").trim(),
            isActive: m?.isActive !== false,
          }))
          .filter((m) => m.key),
      };
    }

    if (update.scheduleByDatePatch !== undefined) {
      const patch = update.scheduleByDatePatch && typeof update.scheduleByDatePatch === "object"
        ? update.scheduleByDatePatch
        : {};

      const existingPoint = await PickupPoint.findById(id).lean();
      if (!existingPoint) {
        return res.status(404).json({ ok: false, error: "Pickup point not found" });
      }

      const currentSchedule =
        existingPoint.scheduleByDate && typeof existingPoint.scheduleByDate === "object"
          ? { ...existingPoint.scheduleByDate }
          : {};

      Object.entries(patch).forEach(([dateKey, value]) => {
        const periods = Array.isArray(value?.periods)
          ? value.periods
              .map((period) => ({
                openFrom: String(period?.openFrom || period?.from || "").trim(),
                openTo: String(period?.openTo || period?.to || "").trim(),
                from: String(period?.from || period?.openFrom || "").trim(),
                to: String(period?.to || period?.openTo || "").trim(),
              }))
              .filter((period) => period.openFrom && period.openTo)
          : [];

        currentSchedule[dateKey] = {
          isOpen: Boolean(value?.isOpen),
          from: String(value?.from || value?.openFrom || periods[0]?.openFrom || "").trim(),
          to: String(
            value?.to ||
            value?.openTo ||
            periods[periods.length - 1]?.openTo ||
            ""
          ).trim(),
          openFrom: String(value?.openFrom || value?.from || periods[0]?.openFrom || "").trim(),
          openTo: String(
            value?.openTo ||
            value?.to ||
            periods[periods.length - 1]?.openTo ||
            ""
          ).trim(),
          periods,
          note: String(value?.note || "").trim(),
        };
      });

      update.scheduleByDate = currentSchedule;
      delete update.scheduleByDatePatch;
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
    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const cart = await Cart.findOne({ telegramId }).lean();

    const safeCart =
      cart || {
        telegramId,
        items: [],
        checkoutDeliveryType: null,
        checkoutDeliveryMethod: null,
        checkoutPickupPointId: null,
        arrivalTime: null,
      };

    const safeItems = Array.isArray(safeCart.items) ? safeCart.items : [];

    const applied = safeItems.some(
      (it) => Number(it?.referralFirstOrderDiscountPercent || 0) > 0
    );

    const percent = applied
      ? Math.max(
          0,
          ...safeItems.map((it) => Number(it?.referralFirstOrderDiscountPercent || 0))
        )
      : 0;

    const totalDiscountZl = Number(
      safeItems
        .reduce(
          (sum, it) => sum + Number(it?.referralFirstOrderDiscountTotalZl || 0),
          0
        )
        .toFixed(2)
    );

    const totalBeforeDiscount = Number(
      safeItems
        .reduce((sum, it) => {
          const qty = Math.max(1, Number(it?.qty || 1));
          const unitPrice = Number(it?.unitPrice || 0);
          const discountPerItem = Number(it?.referralFirstOrderDiscountPerItem || 0);
          return sum + qty * (unitPrice + discountPerItem);
        }, 0)
        .toFixed(2)
    );

    let reason = null;

    if (!applied) {
      const eligibility = await getIsReferralFirstOrderDiscountEligible(telegramId, safeItems);
      reason = eligibility?.reason || null;
    }

    return res.json({
      ok: true,
      cart: safeCart,
      referralFirstOrderDiscount: {
        eligible: applied
          ? true
          : ![
              "NO_USED_REFERRAL_CODE",
              "INVITER_NOT_FOUND",
              "FIRST_ORDER_ALREADY_DONE",
              "PAID_ORDER_ALREADY_EXISTS",
            ].includes(reason),
        applied,
        percent,
        totalBeforeDiscount,
        totalDiscountZl,
        reason,
      },
    });
  } catch (e) {
    console.error("GET /cart error:", e);
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.delete("/cart/item", async (req, res) => {
  try {
    const telegramId = String(req.body?.telegramId || "").trim();
    const itemKey = String(req.body?.itemKey || "").trim();

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    if (!itemKey) {
      return res.status(400).json({ ok: false, error: "itemKey is required" });
    }

    const [productKeyRaw, flavorKeyRaw] = itemKey.split("__");
    const productKey = String(productKeyRaw || "").trim();
    const flavorKey = String(flavorKeyRaw || "").trim();

    if (!productKey || !flavorKey) {
      return res.status(400).json({ ok: false, error: "itemKey is invalid" });
    }

    const cart = await Cart.findOne({ telegramId });
    if (!cart) {
      return res.json({ ok: true, cart: { telegramId, items: [] } });
    }

    const existingItems = Array.isArray(cart.items) ? cart.items : [];

    const itemToRemove = existingItems.find(
      (it) =>
        String(it?.productKey || "").trim() === productKey &&
        String(it?.flavorKey || "").trim() === flavorKey
    );

    if (!itemToRemove) {
      return res.json({ ok: true, cart });
    }

    const removeQty = Math.max(1, Number(itemToRemove?.qty || 1));

    const normId = (v) => String(v || "").trim().replace(/,+$/, "");
    const toObjId = (v) => {
      const s = normId(v);
      return mongoose.isValidObjectId(s) ? new mongoose.Types.ObjectId(s) : null;
    };

    const [courierPP, inpostPP] = await Promise.all([
      PickupPoint.findOne({ key: { $in: ["delivery", "delivery,"] } }, { _id: 1, key: 1 }).lean(),
      PickupPoint.findOne({ key: { $in: ["delivery-2", "delivery-2,"] } }, { _id: 1, key: 1 }).lean(),
    ]);

    const courierWarehouseId = courierPP?._id || null;
    const inpostWarehouseId = inpostPP?._id || null;

    const stockContextIdFor = ({ type, method, pickupPointId }) => {
      if (type === "pickup") return toObjId(pickupPointId);
      if (type === "delivery") {
        if (method === "inpost") return inpostWarehouseId;
        return courierWarehouseId;
      }
      return null;
    };

    const contextId = stockContextIdFor({
      type: cart.checkoutDeliveryType,
      method: cart.checkoutDeliveryMethod,
      pickupPointId: cart.checkoutPickupPointId,
    });

    const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

    if (contextId) {
      const fkNorm = normFlavorKey(flavorKey);
      const fkCandidates = Array.from(
        new Set([String(flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean))
      );

      const ppCandidates = [contextId, String(contextId)].filter(Boolean);

      await Product.updateOne(
        {
          productKey,
          "flavors.flavorKey": { $in: fkCandidates },
          "flavors.stockByPickupPoint.pickupPointId": { $in: ppCandidates },
        },
        {
          $inc: {
            "flavors.$[f].stockByPickupPoint.$[s].reservedQty": -removeQty,
          },
        },
        {
          arrayFilters: [
            { "f.flavorKey": { $in: fkCandidates } },
            { "s.pickupPointId": { $in: ppCandidates } },
          ],
        }
      );

      await Product.updateOne(
        {
          productKey,
          "flavors.flavorKey": { $in: fkCandidates },
          "flavors.stockByPickupPoint.pickupPointId": { $in: ppCandidates },
        },
        {
          $max: {
            "flavors.$[f].stockByPickupPoint.$[s].reservedQty": 0,
          },
        },
        {
          arrayFilters: [
            { "f.flavorKey": { $in: fkCandidates } },
            { "s.pickupPointId": { $in: ppCandidates } },
          ],
        }
      );
    }

    cart.items = existingItems.filter(
      (it) =>
        !(
          String(it?.productKey || "").trim() === productKey &&
          String(it?.flavorKey || "").trim() === flavorKey
        )
    );

    await cart.save();

    return res.json({ ok: true, cart });
  } catch (e) {
    console.error("DELETE /cart/item error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
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

    const checkoutDeliveryType =
      b.checkoutDeliveryType === "pickup" || b.checkoutDeliveryType === "delivery"
        ? b.checkoutDeliveryType
        : null;

    const checkoutDeliveryMethod =
      b.checkoutDeliveryMethod === "courier" || b.checkoutDeliveryMethod === "inpost"
        ? b.checkoutDeliveryMethod
        : null;

      const courierAddress =
        b.courierAddress === null || b.courierAddress === undefined
          ? null
          : String(b.courierAddress || "").trim();

      const arrivalTime =
        b.arrivalTime === null || b.arrivalTime === undefined
          ? null
          : String(b.arrivalTime || "").trim();

      const deliveryPricing = await resolveWarsawDeliveryPricing(courierAddress);
      const courierDistrict = deliveryPricing.districtLabel || null;
      const deliveryFeeZl = deliveryPricing.matched ? Number(deliveryPricing.deliveryFeeZl || 0) : 0;

      const deliveryTimeWindow =
        b.deliveryTimeWindow === null || b.deliveryTimeWindow === undefined
          ? null
          : String(b.deliveryTimeWindow || "").trim();

      const inpostDataRaw = b.inpostData && typeof b.inpostData === "object" ? b.inpostData : {};

      const inpostData = {
        fullName:
          inpostDataRaw.fullName === null || inpostDataRaw.fullName === undefined
            ? null
            : String(inpostDataRaw.fullName || "").trim(),
        phone:
          inpostDataRaw.phone === null || inpostDataRaw.phone === undefined
            ? null
            : String(inpostDataRaw.phone || "").trim(),
        email:
          inpostDataRaw.email === null || inpostDataRaw.email === undefined
            ? null
            : String(inpostDataRaw.email || "").trim(),
        city:
          inpostDataRaw.city === null || inpostDataRaw.city === undefined
            ? null
            : String(inpostDataRaw.city || "").trim(),
        lockerAddress:
          inpostDataRaw.lockerAddress === null || inpostDataRaw.lockerAddress === undefined
            ? null
            : String(inpostDataRaw.lockerAddress || "").trim(),
      };


    const forceCheckoutSelection = !!b.forceCheckoutSelection;

    // минимальная нормализация
    const cleanItemsBase = items
      .map((it) => ({
        productKey: String(it.productKey || "").trim(),
        flavorKey: String(it.flavorKey || "").trim(),
        qty: Math.max(1, Number(it.qty || 1)),

        // цена будет пересчитана ниже по smart-price логике
        unitPrice: Number(it.unitPrice || 0),
        baseUnitPrice: Number(it.baseUnitPrice || it.unitPrice || 0),
        referralFirstOrderDiscountPercent: Number(it.referralFirstOrderDiscountPercent || 0),
        referralFirstOrderDiscountPerItem: Number(it.referralFirstOrderDiscountPerItem || 0),
        referralFirstOrderDiscountTotalZl: Number(it.referralFirstOrderDiscountTotalZl || 0),

        // для UI вкуса
        flavorLabel: String(it.flavorLabel || ""),
        gradient: Array.isArray(it.gradient) ? it.gradient.slice(0, 2) : [],
      }))
      .filter((it) => it.productKey && it.flavorKey);

    const pricingProductKeys = Array.from(
      new Set(cleanItemsBase.map((it) => String(it.productKey || "").trim()).filter(Boolean))
    );

    const pricingProducts = pricingProductKeys.length
      ? await Product.find(
          { productKey: { $in: pricingProductKeys } },
          { productKey: 1, categoryKey: 1, price: 1, title1: 1, title2: 1 }
        ).lean()
      : [];

  const { repricedItems: smartPricedItems, smartPricingMeta } =
    repriceCartItemsWithSmartPricing(cleanItemsBase, pricingProducts);

  const referralFirstOrderDiscountEligibility =
    await getIsReferralFirstOrderDiscountEligible(telegramId, smartPricedItems);

  const {
    items: cleanItems,
    meta: referralFirstOrderDiscountMeta,
  } = referralFirstOrderDiscountEligibility.eligible
    ? applyReferralFirstOrderDiscountToCartItems(
        smartPricedItems,
        referralFirstOrderDiscountEligibility.percent
      )
    : {
        items: smartPricedItems.map((it) => ({
          ...it,
          referralFirstOrderDiscountPercent: 0,
          referralFirstOrderDiscountPerItem: 0,
          referralFirstOrderDiscountTotalZl: 0,
        })),
        meta: {
          applied: false,
          usedCode: String(referralFirstOrderDiscountEligibility.usedCode || "").trim(),
          percent: 0,
          totalBeforeDiscount: referralFirstOrderDiscountEligibility.totalBeforeDiscount,
          totalDiscountZl: 0,
          reason: referralFirstOrderDiscountEligibility.reason,
        },
      };

    const existing = await Cart.findOne({ telegramId }).lean();

    const prevType = existing?.checkoutDeliveryType ?? null;
    const prevMethod = existing?.checkoutDeliveryMethod ?? null;
    const prevPickup = existing?.checkoutPickupPointId ?? null;

    const finalCheckoutDeliveryType =
      forceCheckoutSelection && checkoutDeliveryType
        ? checkoutDeliveryType
        : String(existing?.checkoutDeliveryType || checkoutDeliveryType || "pickup");

    const finalCheckoutDeliveryMethod =
      finalCheckoutDeliveryType === "delivery"
        ? (forceCheckoutSelection && checkoutDeliveryMethod
            ? checkoutDeliveryMethod
            : String(existing?.checkoutDeliveryMethod || checkoutDeliveryMethod || "courier"))
        : "courier";

    const products = await Product.find(
      {
        productKey: {
          $in: [...new Set(cleanItems.map((it) => String(it?.productKey || "").trim()).filter(Boolean))],
        },
      },
      {
        productKey: 1,
        price: 1,
        categoryKey: 1,
      }
    ).lean();


const inpostPricing =
  finalCheckoutDeliveryType === "delivery" && finalCheckoutDeliveryMethod === "inpost"
    ? resolveInpostDeliveryPricing(cleanItems, products)
    : { packageUnits: 0, deliveryFeeZl: 0 };

    const finalCheckoutPickupPointId =
      forceCheckoutSelection ? checkoutPickupPointId : (prevPickup ?? checkoutPickupPointId ?? null);

    // ✅ Guard: pickup requires a pickup point when cart has items
    if (finalCheckoutDeliveryType === "pickup" && !finalCheckoutPickupPointId && cleanItems.length > 0) {
      return res.status(400).json({
        ok: false,
        error: "pickupPointId is required for pickup when cart has items",
      });
    }

    // if (
    //   forceCheckoutSelection &&
    //   finalCheckoutDeliveryType === "delivery" &&
    //   finalCheckoutDeliveryMethod === "courier" &&
    //   cleanItems.length > 0
    // ) {
    //   if (!String(courierAddress || "").trim()) {
    //     return res.status(400).json({
    //       ok: false,
    //       field: "courierAddress",
    //       error: "Для доставки курьером нужно заполнить адрес доставки.",
    //     });
    //   }

    //   if (!String(deliveryTimeWindow || "").trim()) {
    //     return res.status(400).json({
    //       ok: false,
    //       field: "deliveryTimeWindow",
    //       error: "Для доставки курьером нужно выбрать временной промежуток",
    //     });
    //   }
    // }

    // ================= STOCK RESERVATION (reservedQty) =================
    // Goal: when items are in the cart, we reserve their qty on the selected stock context
    // (pickup point OR delivery warehouse), so other users can't over-buy.

    const normPPKey = (v) => String(v || "").trim().toLowerCase().replace(/,+$/, "");

    // Delivery warehouses are stored as PickupPoints with key "delivery" and "delivery-2"
    const [courierPP, inpostPP] = await Promise.all([
      PickupPoint.findOne({ key: { $in: ["delivery", "delivery,"] } }, { _id: 1, key: 1 }).lean(),
      PickupPoint.findOne({ key: { $in: ["delivery-2", "delivery-2,"] } }, { _id: 1, key: 1 }).lean(),
    ]);

    const courierWarehouseId = courierPP?._id || null;
    const inpostWarehouseId = inpostPP?._id || null;

    const normId = (v) => String(v || "").trim().replace(/,+$/, "");
    const toObjId = (v) => {
      const s = normId(v);
      return mongoose.isValidObjectId(s) ? new mongoose.Types.ObjectId(s) : null;
    };

    const stockContextIdFor = ({ type, method, pickupPointId }) => {
      if (type === "pickup") return toObjId(pickupPointId);
      if (type === "delivery") {
        if (method === "inpost") return inpostWarehouseId;
        return courierWarehouseId;
      }
      return null;
    };

    const prevContextId = stockContextIdFor({
      type: prevType,
      method: prevMethod,
      pickupPointId: prevPickup,
    });

    const nextContextId = stockContextIdFor({
      type: finalCheckoutDeliveryType,
      method: finalCheckoutDeliveryMethod,
      pickupPointId: finalCheckoutPickupPointId,
    });

    // ===== DEBUG: stock context mismatch catcher =====
    const dbg = {
      telegramId,
      prev: {
        type: prevType,
        method: prevMethod,
        pickupPointId: prevPickup,
        contextId: prevContextId ? String(prevContextId) : null,
      },
      next: {
        type: finalCheckoutDeliveryType,
        method: finalCheckoutDeliveryMethod,
        pickupPointId: finalCheckoutPickupPointId,
        contextId: nextContextId ? String(nextContextId) : null,
      },
      deliveryWarehouses: {
        courierWarehouseId: courierWarehouseId ? String(courierWarehouseId) : null,
        inpostWarehouseId: inpostWarehouseId ? String(inpostWarehouseId) : null,
      },
      cartCounts: {
        prevItems: Array.isArray(existing?.items) ? existing.items.length : 0,
        nextItems: Array.isArray(cleanItems) ? cleanItems.length : 0,
      },
    };

    console.log("[CART][CTX]", JSON.stringify(dbg));

    if (cleanItems.length && !nextContextId) {
      console.warn("[CART][CTX][WARN] Items present but nextContextId is null — reservation will NOT be applied", JSON.stringify(dbg));
    }

    if (prevContextId && nextContextId && String(prevContextId) !== String(nextContextId)) {
      console.warn("[CART][CTX][WARN] Context changed — will release prev and reserve next", JSON.stringify(dbg));
    }
    // ===== /DEBUG =====

    const sumItems = (itemsArr) => {
      const map = new Map();
      for (const it of Array.isArray(itemsArr) ? itemsArr : []) {
        const pk = String(it.productKey || "").trim();
        const fk = String(it.flavorKey || "").trim();
        if (!pk || !fk) continue;
        const key = `${pk}__${fk}`;
        const qty = Math.max(1, Number(it.qty || 1));
        map.set(key, (map.get(key) || 0) + qty);
      }
      return map;
    };

    const prevSum = sumItems(existing?.items);
    const nextSum = sumItems(cleanItems);

    const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

    const checkReserveAvailable = async ({ productKey, flavorKey, pickupPointId, delta }) => {
    const normId = (v) => String(v || "").trim().replace(/,+$/, "");
    const toObjId = (v) => {
      if (v instanceof mongoose.Types.ObjectId) return v;
      if (v && typeof v === "object" && mongoose.isValidObjectId(String(v))) {
        return new mongoose.Types.ObjectId(String(v));
      }
      const s = normId(v);
      return mongoose.isValidObjectId(s) ? new mongoose.Types.ObjectId(s) : null;
    };

  const ppObj = toObjId(pickupPointId);
  if (!ppObj) return;
  if (!Number.isFinite(delta) || delta <= 0) return;

  const fkNorm = normFlavorKey(flavorKey);
  const fkCandidates = Array.from(
    new Set([String(flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean))
  );

  const prod = await Product.findOne(
    { productKey, "flavors.flavorKey": { $in: fkCandidates } },
    { flavors: 1 }
  ).lean();

  const fl = (prod?.flavors || []).find((f) =>
    fkCandidates.includes(String(f?.flavorKey || "").trim())
  );

  if (!fl) {
    const err = new Error("RESERVE_CONFLICT");
    err.meta = {
      productKey,
      flavorKey: fkCandidates[0],
      pickupPointId: String(ppObj),
      total: 0,
      reserved: 0,
      delta,
      reason: "FLAVOR_NOT_FOUND",
    };
    throw err;
  }

  const row = (fl.stockByPickupPoint || []).find(
    (s) => String(s?.pickupPointId) === String(ppObj)
  );

  const total = Number(row?.totalQty || 0);
  const reserved = Number(row?.reservedQty || 0);
  const available = Math.max(0, total - reserved);

  if (available < delta) {
    const err = new Error("RESERVE_CONFLICT");
    err.meta = {
      productKey,
      flavorKey: fkCandidates[0],
      pickupPointId: String(ppObj),
      total,
      reserved,
      delta,
      reason: "NOT_ENOUGH_AVAILABLE",
    };
    throw err;
  }
};

    const applyReservedDelta = async ({ productKey, flavorKey, pickupPointId, delta }) => {
      const normId = (v) => String(v || "").trim().replace(/,+$/, "");
      const toObjId = (v) => {
        if (v instanceof mongoose.Types.ObjectId) return v;
        if (v && typeof v === "object" && mongoose.isValidObjectId(String(v))) {
          return new mongoose.Types.ObjectId(String(v));
        }
        const s = normId(v);
        return mongoose.isValidObjectId(s) ? new mongoose.Types.ObjectId(s) : null;
      };

      const ppObj = toObjId(pickupPointId);
      if (!ppObj) return;
      if (!Number.isFinite(delta) || delta === 0) return;

      const fkNorm = normFlavorKey(flavorKey);
      const fkCandidates = Array.from(
        new Set([String(flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean))
      );

      console.log("[CART][RESERVE][DELTA]", {
        telegramId,
        productKey,
        flavorKey,
        pickupPointId: String(ppObj),
        delta,
        fkCandidates,
      });

      const session = await mongoose.startSession();
      const MAX_RETRIES = 3;

      try {
        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
          try {
            await session.withTransaction(async () => {
              // 1) читаем нужные данные (внутри транзакции)
              const prod = await Product.findOne(
                { productKey, "flavors.flavorKey": { $in: fkCandidates } },
                { flavors: 1 }
              ).session(session).lean();

              const fl = (prod?.flavors || []).find((f) =>
                fkCandidates.includes(String(f?.flavorKey || "").trim())
              );

              if (!fl) return;

              let row = (fl.stockByPickupPoint || []).find(
                (s) => String(s?.pickupPointId) === String(ppObj)
              );

              // 2) если строки склада нет — создаём (важно для delta > 0)
              if (!row) {
                await Product.updateOne(
                  { productKey, "flavors.flavorKey": { $in: fkCandidates } },
                  {
                    $push: {
                      "flavors.$[f].stockByPickupPoint": {
                        pickupPointId: ppObj,
                        totalQty: 0,
                        reservedQty: 0,
                      },
                    },
                  },
                  {
                    session,
                    arrayFilters: [{ "f.flavorKey": { $in: fkCandidates } }],
                  }
                );

                // перечитываем строку после push
                const prod2 = await Product.findOne(
                  { productKey, "flavors.flavorKey": { $in: fkCandidates } },
                  { flavors: 1 }
                ).session(session).lean();

                const fl2 = (prod2?.flavors || []).find((f) =>
                  fkCandidates.includes(String(f?.flavorKey || "").trim())
                );

                row = (fl2?.stockByPickupPoint || []).find(
                  (s) => String(s?.pickupPointId) === String(ppObj)
                );
              }

              const total = Number(row?.totalQty || 0);
              const reserved = Number(row?.reservedQty || 0);

              // 3) ГАРД: не даём зарезервировать больше доступного
              if (delta > 0) {
                const available = Math.max(0, total - reserved);
                if (available < delta) {
                  const err = new Error("RESERVE_CONFLICT");
                  err.meta = {
                    productKey,
                    flavorKey: fkCandidates[0],
                    pickupPointId: String(ppObj),
                    total,
                    reserved,
                    delta,
                  };
                  throw err;
                }
              }

              // 4) инкремент резерва
              await Product.updateOne(
                {
                  productKey,
                  "flavors.flavorKey": { $in: fkCandidates },
                  "flavors.stockByPickupPoint.pickupPointId": ppObj,
                },
                {
                  $inc: {
                    "flavors.$[f].stockByPickupPoint.$[s].reservedQty": delta,
                  },
                },
                {
                  session,
                  arrayFilters: [
                    { "f.flavorKey": { $in: fkCandidates } },
                    { "s.pickupPointId": ppObj },
                  ],
                }
              );

              // 5) защита от отрицательного резерва (на всякий случай)
              await Product.updateOne(
                { productKey },
                [
                  {
                    $set: {
                      flavors: {
                        $map: {
                          input: "$flavors",
                          as: "f",
                          in: {
                            $cond: [
                              { $in: ["$$f.flavorKey", fkCandidates] },
                              {
                                $mergeObjects: [
                                  "$$f",
                                  {
                                    stockByPickupPoint: {
                                      $map: {
                                        input: "$$f.stockByPickupPoint",
                                        as: "s",
                                        in: {
                                          $cond: [
                                            { $eq: ["$$s.pickupPointId", ppObj] },
                                            {
                                              $mergeObjects: [
                                                "$$s",
                                                { reservedQty: { $max: [0, "$$s.reservedQty"] } },
                                              ],
                                            },
                                            "$$s",
                                          ],
                                        },
                                      },
                                    },
                                  },
                                ],
                              },
                              "$$f",
                            ],
                          },
                        },
                      },
                    },
                  },
                ],
                { session }
              );
            });

            // успех — выходим из retry loop
            return;
          } catch (e) {
            if (e && String(e.message) === "RESERVE_CONFLICT") throw e;

            const msg = String(e?.message || "");
            const isTransient =
              msg.includes("WriteConflict") ||
              msg.includes("TransientTransactionError") ||
              msg.includes("write conflict");

            if (isTransient && attempt < MAX_RETRIES) continue;

            throw e;
          }
        }
      } finally {
        try { session.endSession(); } catch {}
      }
    };

    // Build reservation deltas
    const deltas = [];

    if (prevContextId && nextContextId && String(prevContextId) === String(nextContextId)) {
      // same context: apply only diffs
      const allKeys = new Set([...prevSum.keys(), ...nextSum.keys()]);
      for (const k of allKeys) {
        const [productKey, flavorKey] = k.split("__");
        const before = prevSum.get(k) || 0;
        const after = nextSum.get(k) || 0;
        const delta = after - before;
        if (delta !== 0) deltas.push({ productKey, flavorKey, pickupPointId: nextContextId, delta });
      }
    } else {
      // context changed (or one is missing): release prev, reserve next
      if (prevContextId) {
        for (const [k, qty] of prevSum.entries()) {
          const [productKey, flavorKey] = k.split("__");
          deltas.push({ productKey, flavorKey, pickupPointId: prevContextId, delta: -qty });
        }
      }
      if (nextContextId) {
        for (const [k, qty] of nextSum.entries()) {
          const [productKey, flavorKey] = k.split("__");
          deltas.push({ productKey, flavorKey, pickupPointId: nextContextId, delta: qty });
        }
      }
    }

    // Apply deltas sequentially (simple + safe). If you ever need speed, we can batch later.
for (const d of deltas) {
  if (Number(d.delta) <= 0) continue;

  try {
    await checkReserveAvailable(d);
  } catch (e) {
    if (e?.message === "RESERVE_CONFLICT") {
      return res.status(409).json({
        ok: false,
        error: "OUT_OF_STOCK",
        message: "Not enough stock to reserve items",
        meta: e.meta || null,
      });
    }

    return res.status(500).json({
      ok: false,
      error: "RESERVE_CHECK_FAILED",
      message: "Failed to check item reserve",
    });
  }
}

for (const d of deltas) {
  try {
    await applyReservedDelta(d);
  } catch (e) {
    if (e?.message === "RESERVE_CONFLICT") {
      return res.status(409).json({
        ok: false,
        error: "OUT_OF_STOCK",
        message: "Not enough stock to reserve items",
        meta: e.meta || null,
      });
    }

    console.error("reservedQty update failed", d, e);
    return res.status(500).json({
      ok: false,
      error: "RESERVE_UPDATE_FAILED",
      message: "Failed to update item reserve",
    });
  }
}
    // ================= END STOCK RESERVATION =================

    console.log("[CART][SAVE][FINAL]", {
  telegramId,
  deltas,
  cleanItems,
});

    const updated = await Cart.findOneAndUpdate(
      { telegramId },
      {
        $set: {
          telegramId,
          items: cleanItems,
          checkoutDeliveryType: finalCheckoutDeliveryType,
          checkoutDeliveryMethod: finalCheckoutDeliveryMethod,
          checkoutPickupPointId: finalCheckoutPickupPointId,

          courierAddress,
          inpostData,
          arrivalTime,
          deliveryTimeWindow,
          
          courierDistrict:
            finalCheckoutDeliveryType === "delivery" && finalCheckoutDeliveryMethod === "courier"
              ? courierDistrict
              : null,

          deliveryFeeZl:
            finalCheckoutDeliveryType === "delivery" && finalCheckoutDeliveryMethod === "courier"
              ? deliveryFeeZl
              : 0,

          inpostDeliveryFeeZl:
            finalCheckoutDeliveryType === "delivery" && finalCheckoutDeliveryMethod === "inpost"
              ? Number(inpostPricing.deliveryFeeZl || 0)
              : 0,

          inpostPackageUnits:
            finalCheckoutDeliveryType === "delivery" && finalCheckoutDeliveryMethod === "inpost"
              ? Number(inpostPricing.packageUnits || 0)
              : 0,
        },
      },
      { upsert: true, new: true }
    ).lean();

    return res.json({
      ok: true,
      cart: updated,
      smartPricingMeta,
      referralFirstOrderDiscount: {
        eligible: referralFirstOrderDiscountEligibility.eligible,
        applied: Boolean(referralFirstOrderDiscountMeta?.applied),
        usedCode: String(referralFirstOrderDiscountMeta?.usedCode || "").trim(),
        percent: Number(referralFirstOrderDiscountMeta?.percent || 0),
        totalBeforeDiscount: Number(referralFirstOrderDiscountMeta?.totalBeforeDiscount || 0),
        totalDiscountZl: Number(referralFirstOrderDiscountMeta?.totalDiscountZl || 0),
        reason: referralFirstOrderDiscountEligibility.reason || null,
      },
    });
  } catch (e) {
    console.error("PUT /cart error:", e);
    if (String(e?.message || "").trim() === "RESERVE_CONFLICT") {
  return res.status(409).json({
    ok: false,
    error: "OUT_OF_STOCK",
    meta: e?.meta || null,
  });
}
    res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.post("/orders/confirm", async (req, res) => {
  console.time("orders/confirm total");
  try {
    const telegramId = String(req.body?.telegramId || "").trim();
    if (!telegramId) return res.status(400).json({ ok: false, error: "telegramId is required" });

  console.time("orders/confirm load cart+user");

  const [cart, user] = await Promise.all([
    Cart.findOne({ telegramId }).lean(),
    User.findOne(
      { telegramId },
      { telegramId: 1, referral: 1 }
    ).lean(),
  ]);

  console.timeEnd("orders/confirm load cart+user");

    const referralDiscountMeta = {
      applied: Array.isArray(cart?.items)
        ? cart.items.some((it) => Number(it?.referralFirstOrderDiscountTotalZl || 0) > 0)
        : false,

      usedCode: String(user?.referral?.usedCode || "").trim(),

      percent: Array.isArray(cart?.items)
        ? Number(
            cart.items.find((it) => Number(it?.referralFirstOrderDiscountPercent || 0) > 0)
              ?.referralFirstOrderDiscountPercent || 0
          )
        : 0,

      totalDiscountZl: Number(
        (Array.isArray(cart?.items) ? cart.items : []).reduce((sum, it) => {
          return sum + Number(it?.referralFirstOrderDiscountTotalZl || 0);
        }, 0).toFixed(2)
      ),

      totalBeforeDiscount: Number(
        (Array.isArray(cart?.items) ? cart.items : []).reduce((sum, it) => {
          const qty = Math.max(1, Number(it?.qty || 1));
          const baseUnitPrice = Number(it?.baseUnitPrice || it?.unitPrice || 0);
          return sum + qty * baseUnitPrice;
        }, 0).toFixed(2)
      ),
    };

    if (!cart || !Array.isArray(cart.items) || cart.items.length === 0) {
      return res.status(400).json({ ok: false, error: "Cart is empty" });
    }

    if (cart.checkoutDeliveryType === "delivery" && cart.checkoutDeliveryMethod === "courier") {
      if (!String(cart?.courierAddress || "").trim()) {
        return res.status(400).json({
          ok: false,
          field: "courierAddress",
          error: "Для доставки курьером нужно заполнить адрес доставки.",
        });
      }

      if (!String(cart?.deliveryTimeWindow || "").trim()) {
        return res.status(400).json({
          ok: false,
          field: "deliveryTimeWindow",
          error: "Для доставки курьером нужно выбрать временной промежуток",
        });
      }
    }

    if (cart.checkoutDeliveryType === "delivery" && cart.checkoutDeliveryMethod === "courier") {
      const savedCourierDistrict = String(cart?.courierDistrict || "").trim();
      const savedDeliveryFeeZl = Number(cart?.deliveryFeeZl || 0);

      if (!savedCourierDistrict || savedDeliveryFeeZl <= 0) {
        const deliveryPricing = await resolveWarsawDeliveryPricing(cart?.courierAddress || "");

        if (!deliveryPricing.matched) {
          return res.status(400).json({
            ok: false,
            field: "courierAddress",
            error: "Не удалось определить район Варшавы по адресу. Укажите адрес точнее, например: Puławska 12, Warszawa.",
          });
        }
      }
    }

    // 1) total
    const itemsTotalZl = cart.items.reduce((sum, it) => {
      const qty = Math.max(1, Number(it.qty || 1));
      const price = Number(it.unitPrice || 0);
      return sum + qty * price;
    }, 0);

    const courierDeliveryFeeZl =
      cart.checkoutDeliveryType === "delivery" && cart.checkoutDeliveryMethod === "courier"
        ? Number(cart.deliveryFeeZl || 0)
        : 0;

    const inpostDeliveryFeeZl =
      cart.checkoutDeliveryType === "delivery" && cart.checkoutDeliveryMethod === "inpost"
        ? Number(cart.inpostDeliveryFeeZl || 0)
        : 0;

    const totalZl = Number((itemsTotalZl + courierDeliveryFeeZl + inpostDeliveryFeeZl).toFixed(2));

    // 2) delivery mapping (из Cart -> Order)
    const deliveryType = cart.checkoutDeliveryType === "pickup" ? "pickup" : "delivery";
    const deliveryMethod =
      deliveryType === "delivery"
        ? (cart.checkoutDeliveryMethod === "inpost" ? "inpost" : (cart.checkoutDeliveryMethod === "courier" ? "courier" : null))
        : null;

    const pickupPointId = deliveryType === "pickup" ? (cart.checkoutPickupPointId || null) : null;

    let schedulePoint = null;

    if (deliveryType === "pickup" && pickupPointId) {
      schedulePoint = await PickupPoint.findById(
        pickupPointId,
        { title: 1, address: 1, key: 1, scheduleByDate: 1 }
      ).lean();
    } else if (deliveryType === "delivery" && deliveryMethod === "courier") {
      schedulePoint = await PickupPoint.findOne(
        { key: { $in: ["delivery", "delivery,"] } },
        { title: 1, address: 1, key: 1, scheduleByDate: 1 }
      ).lean();
    } else if (deliveryType === "delivery" && deliveryMethod === "inpost") {
      schedulePoint = await PickupPoint.findOne(
        { key: { $in: ["delivery-2", "delivery-2,"] } },
        { title: 1, address: 1, key: 1, scheduleByDate: 1 }
      ).lean();
    }

    if (schedulePoint) {
      const isCourierDelivery =
        deliveryType === "delivery" && deliveryMethod === "courier";

      const openState = getPointOpenStateNow(schedulePoint);

      const courierWindowFitsSchedule = isCourierDelivery
        ? isTimeWindowInsidePointSchedule(schedulePoint, cart?.deliveryTimeWindow)
        : false;

      if (
        (isCourierDelivery && !courierWindowFitsSchedule) ||
        (!isCourierDelivery && !openState.isOpen)
      ) {
        const pointLabel =
          String(schedulePoint?.title || "").trim() ||
          String(schedulePoint?.address || "").trim() ||
          (deliveryType === "delivery" && deliveryMethod === "courier"
            ? "Курьер"
            : deliveryType === "delivery" && deliveryMethod === "inpost"
            ? "InPost"
            : "Точка самовывоза");

        const scheduleText =
          Array.isArray(openState?.periods) && openState.periods.length
            ? `График сегодня: ${openState.periods
                .map((p) => `${p.openFrom}–${p.openTo}`)
                .join(", ")}.`
            : openState.openFrom && openState.openTo
            ? `График сегодня: ${openState.openFrom}–${openState.openTo}.`
            : `График на сегодня не настроен.`;

        const selectedWindowText =
          isCourierDelivery && String(cart?.deliveryTimeWindow || "").trim()
            ? ` Выбранный промежуток: ${String(cart.deliveryTimeWindow).trim()}.`
            : "";

        return res.status(400).json({
          ok: false,
          field: "schedule",
          error: `${pointLabel} сейчас не принимает заказы. ${scheduleText}${selectedWindowText}`,
        });
      }
    }

    // 3) methodLabel (готовая строка для UI)
    let methodLabel = "";
    if (deliveryType === "pickup") {
      if (pickupPointId) {
        const pp = await PickupPoint.findById(pickupPointId).lean();
        methodLabel = `Самовывоз — ${pp?.title || pp?.address || "Точка"}`;
      } else {
        methodLabel = "Самовывоз";
      }
    } else {
      if (deliveryMethod === "inpost") methodLabel = "Доставка — InPost";
      else if (deliveryMethod === "courier") methodLabel = "Доставка — Курьер";
      else methodLabel = "Доставка";
    }

    // 4) bgUrl from FIRST cart item product
    const first = cart.items[0];
    let bgUrl = "";
    if (first?.productKey) {
      const prod = await Product.findOne(
        { productKey: String(first.productKey) },
        { cardBgUrl: 1 }
      ).lean();

      bgUrl = String(prod?.cardBgUrl || "");
    }

    // 5) Собрать items snapshot в твою структуру (product -> flavors[])
    const productKeys = Array.from(
      new Set(cart.items.map((it) => String(it.productKey || "").trim()).filter(Boolean))
    );

    const products = await Product.find(
      { productKey: { $in: productKeys } },
      {
        _id: 1,
        productKey: 1,
        title1: 1,
        title2: 1,
        orderImgUrl: 1,
        cardBgUrl: 1,
        price: 1,
      }
    ).lean();

    const prodByKey = new Map(products.map((p) => [String(p.productKey), p]));
    const byProduct = new Map(); // productKey -> row

    console.time("orders/confirm build order items");
    for (const it of cart.items) {
      const pk = String(it.productKey || "").trim();
      const fk = String(it.flavorKey || "").trim();
      if (!pk || !fk) continue;

      const qty = Math.max(1, Number(it.qty || 1));
      const unitPrice = Number(it.unitPrice || 0);
      const flavorLabel = String(it.flavorLabel || "");
      const gradient = Array.isArray(it.gradient) ? it.gradient.slice(0, 2) : [];

      const originalBaseUnitPrice = Number(it?.baseUnitPrice || 0);
      const referralFirstOrderDiscountPerItem = Number(it?.referralFirstOrderDiscountPerItem || 0);
      const referralFirstOrderDiscountTotalZl = Number(it?.referralFirstOrderDiscountTotalZl || 0);

      const smartDiscountPerItem = Number(
        Math.max(0, originalBaseUnitPrice - unitPrice - referralFirstOrderDiscountPerItem).toFixed(2)
      );

      const smartDiscountTotalZl = Number(
        Math.max(0, smartDiscountPerItem * qty).toFixed(2)
      );

      const prod = prodByKey.get(pk);
      if (!prod?._id) continue; // если товар не найден — пропускаем

      const baseUnitPrice = Number(prod?.price || unitPrice || 0);

      let row = byProduct.get(pk);
      if (!row) {
        row = {
          productId: prod._id,
          productKey: pk,
          productTitle1: String(prod.title1 || ""),
          productTitle2: String(prod.title2 || ""),
          orderImgUrl: String(prod.orderImgUrl || ""),
          cardBgUrl: String(prod.cardBgUrl || ""),
          flavorsMap: new Map(), // fk -> flavor snapshot
        };
        byProduct.set(pk, row);
      }

      const prev = row.flavorsMap.get(fk);
      if (!prev) {
        row.flavorsMap.set(fk, {
          flavorKey: fk,
          qty,
          unitPrice,
          baseUnitPrice: Number(originalBaseUnitPrice || baseUnitPrice || unitPrice || 0),
          smartDiscountPerItem,
          smartDiscountTotalZl,
          referralFirstOrderDiscountPercent: Number(it?.referralFirstOrderDiscountPercent || 0),
          referralFirstOrderDiscountPerItem,
          referralFirstOrderDiscountTotalZl,
          flavorLabel,
          gradient,
        });
      } else {
        prev.qty += qty;
        if (unitPrice) prev.unitPrice = unitPrice;
        if (baseUnitPrice) prev.baseUnitPrice = Number(originalBaseUnitPrice || baseUnitPrice || unitPrice || 0);

        prev.smartDiscountPerItem = Number(smartDiscountPerItem || prev.smartDiscountPerItem || 0);
        prev.smartDiscountTotalZl = Number(
          (Number(prev.smartDiscountTotalZl || 0) + Number(smartDiscountTotalZl || 0)).toFixed(2)
        );

        prev.referralFirstOrderDiscountPercent = Number(it?.referralFirstOrderDiscountPercent || prev.referralFirstOrderDiscountPercent || 0);
        prev.referralFirstOrderDiscountPerItem = Number(referralFirstOrderDiscountPerItem || prev.referralFirstOrderDiscountPerItem || 0);
        prev.referralFirstOrderDiscountTotalZl = Number(
          (Number(prev.referralFirstOrderDiscountTotalZl || 0) + Number(referralFirstOrderDiscountTotalZl || 0)).toFixed(2)
        );

        if (flavorLabel) prev.flavorLabel = flavorLabel;
        if (gradient.length) prev.gradient = gradient;
      }
    }

    const orderItems = Array.from(byProduct.values()).map((row) => ({
      productId: row.productId,
      productKey: row.productKey,
      productTitle1: row.productTitle1,
      productTitle2: row.productTitle2,
      orderImgUrl: row.orderImgUrl,
      cardBgUrl: row.cardBgUrl,
      flavors: Array.from(row.flavorsMap.values()),
    }));
    console.timeEnd("orders/confirm build order items");

    // ================= STOCK CHECK (avoid context mismatch) =================
    // IMPORTANT: use THE SAME stock context logic as /cart reservations.
    // Product.flavors.stockByPickupPoint.pickupPointId is ObjectId -> always use ObjectId.

    const normId = (v) => String(v || "").trim().replace(/,+$/, "");
    const toObjId = (v) => {
      const s = normId(v);
      return mongoose.isValidObjectId(s) ? new mongoose.Types.ObjectId(s) : null;
    };

    // Delivery warehouses are stored as PickupPoints with key "delivery" and "delivery-2"
    const [courierPP, inpostPP] = await Promise.all([
      PickupPoint.findOne({ key: { $in: ["delivery", "delivery,"] } }, { _id: 1, key: 1 }).lean(),
      PickupPoint.findOne({ key: { $in: ["delivery-2", "delivery-2,"] } }, { _id: 1, key: 1 }).lean(),
    ]);

    const courierWarehouseId = courierPP?._id || null;
    const inpostWarehouseId = inpostPP?._id || null;

    const stockContextIdFor = ({ type, method, pickupPointId }) => {
      if (type === "pickup") return toObjId(pickupPointId);
      if (type === "delivery") {
        if (method === "inpost") return inpostWarehouseId;
        return courierWarehouseId;
      }
      return null;
    };

    const contextId = stockContextIdFor({
      type: cart.checkoutDeliveryType,
      method: cart.checkoutDeliveryMethod,
      pickupPointId: cart.checkoutPickupPointId,
    });

    console.log(
      "[ORDER][CONFIRM][CTX]",
      JSON.stringify({
        telegramId,
        checkoutDeliveryType: cart.checkoutDeliveryType ?? null,
        checkoutDeliveryMethod: cart.checkoutDeliveryMethod ?? null,
        checkoutPickupPointId: cart.checkoutPickupPointId ?? null,
        contextId: contextId ? String(contextId) : null,
        deliveryWarehouses: {
          courierWarehouseId: courierWarehouseId ? String(courierWarehouseId) : null,
          inpostWarehouseId: inpostWarehouseId ? String(inpostWarehouseId) : null,
        },
      })
    );

    if (!contextId) {
      return res.status(400).json({ ok: false, error: "Stock context is not set (pickup point / delivery warehouse)" });
    }

    // Availability check: available = totalQty - reservedQty.
    // BUT reservedQty already includes THIS cart reservation, so for self-check we add back my qty.
    const cartSum = new Map(); // key -> qty
    for (const it of cart.items) {
      const pk = String(it.productKey || "").trim();
      const fk = String(it.flavorKey || "").trim();
      if (!pk || !fk) continue;
      const key = `${pk}__${fk}`;
      const q = Math.max(1, Number(it.qty || 1));
      cartSum.set(key, (cartSum.get(key) || 0) + q);
    }

    const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

    const productKeysForCheck = Array.from(
      new Set(cart.items.map((it) => String(it.productKey || "").trim()).filter(Boolean))
    );

    const productsForCheck = await Product.find(
      { productKey: { $in: productKeysForCheck } },
      { productKey: 1, title1: 1, title2: 1, flavors: 1 }
    ).lean();

    const prodByKey2 = new Map(productsForCheck.map((p) => [String(p.productKey), p]));

    const missing = [];

    for (const [k, myQty] of cartSum.entries()) {
      const [productKey, flavorKey] = k.split("__");
      const p = prodByKey2.get(productKey);

      if (!p) {
        missing.push({ productKey, flavorKey, need: myQty, have: 0, reason: "product_not_found" });
        continue;
      }

      const fkNorm = normFlavorKey(flavorKey);
      const fkCandidates = Array.from(new Set([String(flavorKey).trim(), fkNorm, `${fkNorm},`].filter(Boolean)));
      const flavor = (p.flavors || []).find((f) => fkCandidates.includes(String(f.flavorKey || "").trim()));

      if (!flavor) {
        missing.push({ productKey, flavorKey, need: myQty, have: 0, reason: "flavor_not_found" });
        continue;
      }

      const row = (flavor.stockByPickupPoint || []).find((s) => String(s.pickupPointId) === String(contextId));
      const total = Number(row?.totalQty || 0);
      const reserved = Number(row?.reservedQty || 0);

      // self-check: add back myQty so we don't block ourselves
      const effectiveHave = Math.max(0, total - reserved + myQty);

      if (effectiveHave < myQty) {
        missing.push({
          productKey,
          flavorKey,
          need: myQty,
          have: effectiveHave,
          total,
          reserved,
          contextId: String(contextId),
          reason: "not_enough_stock",
        });
      }
    }

    if (missing.length) {
      console.warn("[ORDER][CONFIRM][STOCK][MISSING]", JSON.stringify({ telegramId, contextId: String(contextId), missing }));
      return res.status(409).json({ ok: false, error: "Not enough stock", missing });
    }

    // ================= /STOCK CHECK =================

    // 6) COMMIT stock: totalQty -= qty AND reservedQty -= qty (ВАЖНО!)
    // const [courierPP, inpostPP] = await Promise.all([
    //   PickupPoint.findOne({ key: { $in: ["delivery", "delivery,"] } }, { _id: 1 }).lean(),
    //   PickupPoint.findOne({ key: { $in: ["delivery-2", "delivery-2,"] } }, { _id: 1 }).lean(),
    // ]);

    // const courierWarehouseId = courierPP?._id || null;
    // const inpostWarehouseId = inpostPP?._id || null;

    // const stockContextIdFor = ({ type, method, pickupPointId }) => {
    //   if (type === "pickup") return pickupPointId || null;
    //   if (type === "delivery") {
    //     if (method === "inpost") return inpostWarehouseId;
    //     return courierWarehouseId;
    //   }
    //   return null;
    // };

    // const contextId = stockContextIdFor({
    //   type: cart.checkoutDeliveryType,
    //   method: cart.checkoutDeliveryMethod,
    //   pickupPointId: cart.checkoutPickupPointId,
    // });

    // const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

    // const applyPurchaseDelta = async ({ productKey, flavorKey, pickupPointId, qty }) => {
    //   const q = Math.max(1, Number(qty || 1));
    //   if (!pickupPointId || !productKey || !flavorKey || !Number.isFinite(q) || q <= 0) return;

    //   const ppIdObj = pickupPointId;
    //   const ppIdStr = String(pickupPointId);

    //   const fkNorm = normFlavorKey(flavorKey);
    //   const fkCandidates = Array.from(new Set([String(flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean)));
    //   const ppCandidates = [ppIdObj, ppIdStr].filter(Boolean);

    //   await Product.updateOne(
    //     {
    //       productKey,
    //       "flavors.flavorKey": { $in: fkCandidates },
    //       "flavors.stockByPickupPoint.pickupPointId": { $in: ppCandidates },
    //     },
    //     {
    //       $inc: {
    //         "flavors.$[f].stockByPickupPoint.$[s].totalQty": -q,
    //         "flavors.$[f].stockByPickupPoint.$[s].reservedQty": -q,
    //       },
    //     },
    //     {
    //       arrayFilters: [
    //         { "f.flavorKey": { $in: fkCandidates } },
    //         { "s.pickupPointId": { $in: ppCandidates } },
    //       ],
    //     }
    //   );

    //   // clamp
    //   await Product.updateOne(
    //     {
    //       productKey,
    //       "flavors.flavorKey": { $in: fkCandidates },
    //       "flavors.stockByPickupPoint.pickupPointId": { $in: ppCandidates },
    //     },
    //     {
    //       $max: {
    //         "flavors.$[f].stockByPickupPoint.$[s].totalQty": 0,
    //         "flavors.$[f].stockByPickupPoint.$[s].reservedQty": 0,
    //       },
    //     },
    //     {
    //       arrayFilters: [
    //         { "f.flavorKey": { $in: fkCandidates } },
    //         { "s.pickupPointId": { $in: ppCandidates } },
    //       ],
    //     }
    //   );
    // };

    // if (contextId) {
    //   for (const it of cart.items) {
    //     const productKey = String(it.productKey || "").trim();
    //     const flavorKey = String(it.flavorKey || "").trim();
    //     const qty = Math.max(1, Number(it.qty || 1));
    //     if (!productKey || !flavorKey) continue;
    //     await applyPurchaseDelta({ productKey, flavorKey, pickupPointId: contextId, qty });
    //   }
    // }

    

    // 7) unique orderNo
    let orderNo = genOrderNo();
    for (let i = 0; i < 5; i++) {
      const exists = await Order.findOne({ orderNo }, { _id: 1 }).lean();
      if (!exists) break;
      orderNo = genOrderNo();
    }

    // 8) create order

    const confirmedDeliveryPricing =
      deliveryType === "delivery" && deliveryMethod === "courier"
        ? (
            String(cart?.courierDistrict || "").trim() && Number(cart?.deliveryFeeZl || 0) > 0
              ? {
                  districtLabel: String(cart.courierDistrict || "").trim(),
                  deliveryFeeZl: Number(cart.deliveryFeeZl || 0),
                }
              : await resolveWarsawDeliveryPricing(cart.courierAddress || "")
          )
        : { districtLabel: null, deliveryFeeZl: 0 };

    const confirmedInpostPricing =
      deliveryType === "delivery" && deliveryMethod === "inpost"
        ? {
            packageUnits: Number(cart?.inpostPackageUnits || 0),
            deliveryFeeZl: Number(cart?.inpostDeliveryFeeZl || 0),
          }
        : { packageUnits: 0, deliveryFeeZl: 0 };

    const duplicateCreatedAfter = new Date(Date.now() - 15 * 1000);

    const currentOrderFingerprint = JSON.stringify({
      telegramId,
      totalZl: Number(totalZl.toFixed(2)),
      deliveryType,
      deliveryMethod,
      pickupPointId: pickupPointId ? String(pickupPointId) : null,
      arrivalTime: cart.arrivalTime ?? null,
      deliveryTimeWindow: cart.deliveryTimeWindow ?? null,
      courierAddress: cart.courierAddress ?? null,
      inpostData: cart.inpostData ?? {},
      items: orderItems.map((row) => ({
        productKey: String(row?.productKey || ""),
        flavors: (Array.isArray(row?.flavors) ? row.flavors : []).map((f) => ({
          flavorKey: String(f?.flavorKey || ""),
          qty: Number(f?.qty || 0),
          unitPrice: Number(f?.unitPrice || 0),
        })),
      })),
    });

    console.time("orders/confirm duplicate check");
    const recentDuplicateCandidates = await Order.find(
      {
        userTelegramId: telegramId,
        totalZl: Number(totalZl.toFixed(2)),
        deliveryType,
        deliveryMethod,
        pickupPointId,
        status: "created",
        createdAt: { $gte: duplicateCreatedAfter },
      },
      {
        _id: 1,
        userTelegramId: 1,
        totalZl: 1,
        deliveryType: 1,
        deliveryMethod: 1,
        pickupPointId: 1,
        arrivalTime: 1,
        deliveryTimeWindow: 1,
        courierAddress: 1,
        inpostData: 1,
        items: 1,
        createdAt: 1,
      }
    )
      .sort({ createdAt: -1 })
      .lean();

    const duplicateOrder = recentDuplicateCandidates.find((existing) => {
      const existingFingerprint = JSON.stringify({
        telegramId: String(existing?.userTelegramId || "").trim(),
        totalZl: Number(existing?.totalZl || 0),
        deliveryType: existing?.deliveryType || null,
        deliveryMethod: existing?.deliveryMethod || null,
        pickupPointId: existing?.pickupPointId ? String(existing.pickupPointId) : null,
        arrivalTime: existing?.arrivalTime ?? null,
        deliveryTimeWindow: existing?.deliveryTimeWindow ?? null,
        courierAddress: existing?.courierAddress ?? null,
        inpostData: existing?.inpostData ?? {},
        items: (Array.isArray(existing?.items) ? existing.items : []).map((row) => ({
          productKey: String(row?.productKey || ""),
          flavors: (Array.isArray(row?.flavors) ? row.flavors : []).map((f) => ({
            flavorKey: String(f?.flavorKey || ""),
            qty: Number(f?.qty || 0),
            unitPrice: Number(f?.unitPrice || 0),
          })),
        })),
      });

      return existingFingerprint === currentOrderFingerprint;
    });

    console.timeEnd("orders/confirm duplicate check");
    if (duplicateOrder) {
      return res.json({ ok: true, order: duplicateOrder, duplicate: true });
    }
    
    console.time("orders/confirm create order")
    const created = await Order.create({
      userTelegramId: telegramId,

      orderNo,
      totalZl: Number(totalZl.toFixed(2)),
      currency: "PLN",

      bgUrl,
      methodLabel,

      deliveryType,
      deliveryMethod,
      pickupPointId,

      arrivalTime: cart.arrivalTime ?? null,
      deliveryTimeWindow: cart.deliveryTimeWindow ?? null,
      courierAddress: cart.courierAddress ?? null,
      inpostData: cart.inpostData ?? {},

      courierDistrict:
        deliveryType === "delivery" && deliveryMethod === "courier"
          ? (confirmedDeliveryPricing.districtLabel || cart.courierDistrict || null)
          : null,

      deliveryFeeZl:
        deliveryType === "delivery" && deliveryMethod === "courier"
          ? Number(confirmedDeliveryPricing.deliveryFeeZl || cart.deliveryFeeZl || 0)
          : 0,

      inpostDeliveryFeeZl:
        deliveryType === "delivery" && deliveryMethod === "inpost"
          ? Number(confirmedInpostPricing.deliveryFeeZl || 0)
          : 0,

      inpostPackageUnits:
        deliveryType === "delivery" && deliveryMethod === "inpost"
          ? Number(confirmedInpostPricing.packageUnits || 0)
          : 0,

      items: orderItems,

      payment: {
        status: "unpaid",
        amountZl: Number(totalZl.toFixed(2)),
        referralUsedCode: referralDiscountMeta.usedCode,
        referralFirstOrderDiscountApplied: Boolean(referralDiscountMeta.applied),
        referralFirstOrderDiscountPercent: Number(referralDiscountMeta.percent || 0),
        referralFirstOrderDiscountTotalZl: Number(referralDiscountMeta.totalDiscountZl || 0),
        subtotalBeforeReferralDiscountZl: Number(referralDiscountMeta.totalBeforeDiscount || 0),
        totalBeforeReferralDiscountZl: Number(referralDiscountMeta.totalBeforeDiscount || 0),
      },

      status: "created",
      // ✅ заказ создан: товар остаётся в reservedQty (как в корзине)
      stockReservedAt: new Date(),
      stockCommittedAt: null,
      stockReleasedAt: null,
    });

    // 9) clear cart
    console.time("orders/confirm clear cart");
    await Cart.updateOne(
      { telegramId },
      {
        $set: {
          items: [],
          checkoutDeliveryType: null,
          checkoutDeliveryMethod: null,
          checkoutPickupPointId: null,
          arrivalTime: null,
          deliveryTimeWindow: null,
          courierAddress: null,
          courierDistrict: null,
          deliveryFeeZl: 0,
          inpostDeliveryFeeZl: 0,
          inpostPackageUnits: 0,
          inpostData: {
            fullName: null,
            phone: null,
            email: null,
            city: null,
            lockerAddress: null,
          },
        },
      }
    );
    console.timeEnd("orders/confirm clear cart");

    console.timeEnd("orders/confirm total");
    res.json({ ok: true, order: created });

    Promise.resolve()
      .then(() => sendClientOrderCreatedInfo(created))
      .catch((e) => console.error("sendClientOrderCreatedInfo post-response error:", e));

    Promise.resolve()
      .then(() => startPaymentReminder(created))
      .catch((e) => console.error("startPaymentReminder post-response error:", e));

    // Promise.resolve()
    //   .then(() => sendOrderCreatedNotification(created))
    //   .catch((e) => console.error("sendOrderCreatedNotification post-response error:", e));

    return;
  } catch (e) {
    console.error("POST /orders/confirm error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Public: cancel order by user =====
app.post("/orders/:id/cancel", async (req, res) => {
  try {
    const orderId = String(req.params.id || "").trim();
    const telegramId = String(req.body?.telegramId || "").trim();

    if (!orderId) {
      return res.status(400).json({ ok: false, error: "orderId is required" });
    }

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const order = await Order.findById(orderId);
    if (!order) {
      return res.status(404).json({ ok: false, error: "Order not found" });
    }

    if (String(order.userTelegramId || "") !== telegramId) {
      return res.status(403).json({ ok: false, error: "FORBIDDEN" });
    }

    const status = String(order.status || "").toLowerCase();

    if (status === "completed") {
      return res.status(400).json({
        ok: false,
        error: "COMPLETED_ORDER_CANNOT_BE_CANCELED",
      });
    }

    if (status === "canceled") {
      return res.json({ ok: true, order });
    }

    if (!order.stockReleasedAt) {
      await releaseOrderReservedStock(order);
      order.stockReleasedAt = new Date();
    }

    await refundOrderCashback(order);

    const freshOrderAfterRefund = await Order.findById(order._id);
    if (!freshOrderAfterRefund) {
      throw new Error("ORDER_NOT_FOUND_AFTER_REFUND");
    }

    order.payment = {
      ...(freshOrderAfterRefund.payment?.toObject
        ? freshOrderAfterRefund.payment.toObject()
        : freshOrderAfterRefund.payment || {}),
      status: "unpaid",
      paidAt: null,
      checkedAt: new Date(),
      checkedByTelegramId: telegramId,
    };

    order.status = "canceled";
    order.canceledAt = new Date();
    order.canceledByTelegramId = telegramId;

    await order.save();

    await refreshManagerOrderMessage(order);

    try {
      stopPaymentReminder(order._id);
    } catch {}

    return res.json({ ok: true, order });
  } catch (e) {
    console.error("POST /orders/:id/cancel error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Repeat order (create new order from existing snapshot) =====
app.post("/orders/repeat", async (req, res) => {
  try {
    const telegramId = String(req.body?.telegramId || "").trim();
    const orderNo = req.body?.orderNo ? String(req.body.orderNo).trim() : null;
    const orderId = req.body?.orderId ? String(req.body.orderId).trim() : null;

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    if (!orderNo && !orderId) {
      return res.status(400).json({ ok: false, error: "orderNo or orderId is required" });
    }

    const orig = await Order.findOne(
      { userTelegramId: telegramId, ...(orderId ? { _id: orderId } : { orderNo }) },
      {
        deliveryType: 1,
        deliveryMethod: 1,
        pickupPointId: 1,
        arrivalTime: 1,
        courierAddress: 1,
        inpostData: 1,
        items: 1,
      }
    ).lean();

    if (!orig) {
      return res.status(404).json({ ok: false, error: "Order not found" });
    }

    const repeatedItems = [];

    for (const p of Array.isArray(orig.items) ? orig.items : []) {
      const productKey = String(p?.productKey || "").trim();
      if (!productKey) continue;

      for (const f of Array.isArray(p?.flavors) ? p.flavors : []) {
        const flavorKey = String(f?.flavorKey || "").trim();
        if (!flavorKey) continue;

        repeatedItems.push({
          productKey,
          flavorKey,
          qty: Math.max(1, Number(f?.qty || 1)),
          unitPrice: Number(f?.unitPrice || 0),
          flavorLabel: String(f?.flavorLabel || ""),
          gradient: Array.isArray(f?.gradient) ? f.gradient.slice(0, 2) : [],
        });
      }
    }

    if (!repeatedItems.length) {
      return res.status(400).json({ ok: false, error: "Order has no items" });
    }

    const allPoints = await PickupPoint.find({}, { _id: 1, key: 1, title: 1, address: 1 }).lean();
    const pointById = new Map(allPoints.map((p) => [String(p._id), p]));
    const pointByKey = new Map(
      allPoints.map((p) => [String(p.key || "").trim().replace(/,+$/, ""), p])
    );

    const normFlavorKey = (v) => String(v || "").trim().replace(/,+$/, "");

    const makePointLabel = (point) => {
      const k = String(point?.key || "").trim().replace(/,+$/, "");
      if (k === "delivery") return "Доставка — Курьер";
      if (k === "delivery-2") return "Доставка — InPost";
      return point?.address || point?.title || "Точка";
    };

    const targetPoint =
      orig.deliveryType === "pickup"
        ? pointById.get(String(orig.pickupPointId || "")) || null
        : pointByKey.get(orig.deliveryMethod === "inpost" ? "delivery-2" : "delivery") || null;

    const targetContextId = targetPoint?._id ? String(targetPoint._id) : "";
    const targetLabel = makePointLabel(targetPoint);

    const missing = [];

    for (const it of repeatedItems) {
      const productKey = String(it.productKey || "").trim();
      const flavorKey = String(it.flavorKey || "").trim();
      if (!productKey || !flavorKey) continue;

      const fkNorm = normFlavorKey(flavorKey);
      const fkCandidates = Array.from(
        new Set([String(flavorKey || "").trim(), fkNorm, `${fkNorm},`].filter(Boolean))
      );

      const prod = await Product.findOne(
        { productKey, "flavors.flavorKey": { $in: fkCandidates } },
        { productKey: 1, title1: 1, title2: 1, flavors: 1 }
      ).lean();

      const fl = (prod?.flavors || []).find((f) =>
        fkCandidates.includes(String(f?.flavorKey || "").trim())
      );

      const row = (fl?.stockByPickupPoint || []).find(
        (s) => String(s?.pickupPointId) === String(targetContextId)
      );

      const total = Number(row?.totalQty || 0);
      const reserved = Number(row?.reservedQty || 0);
      const available = Math.max(0, total - reserved);
      const requested = Math.max(1, Number(it.qty || 1));

      if (available >= requested) continue;

      const productTitle =
        [prod?.title1, prod?.title2].filter(Boolean).join(" ").trim() || productKey;

      const flavorLabel = String(
        it.flavorLabel || fl?.label || fl?.flavorKey || flavorKey
      ).trim();

      const alternatives = (fl?.stockByPickupPoint || [])
        .map((s) => {
          const point = pointById.get(String(s?.pickupPointId || ""));
          const altAvailable = Math.max(
            0,
            Number(s?.totalQty || 0) - Number(s?.reservedQty || 0)
          );

          return {
            pointId: String(s?.pickupPointId || ""),
            label: makePointLabel(point),
            available: altAvailable,
          };
        })
        .filter((x) => x.pointId && x.pointId !== String(targetContextId) && x.available > 0)
        .sort((a, b) => b.available - a.available)
        .slice(0, 6);

      missing.push({
        productTitle,
        flavorLabel,
        requested,
        available,
        alternatives,
      });
    }

    if (missing.length) {
      const message = [
        `На «${targetLabel}» сейчас недостаточно наличия для повторного заказа.`,
        ``,
        ...missing.flatMap((m) => {
          const head = `• ${m.productTitle} — ${m.flavorLabel}: нужно ${m.requested} шт., доступно ${m.available} шт.`;

          if (!m.alternatives.length) {
            return [head, `  Альтернатива: выберите другой вкус, позицию или другой склад.`];
          }

          return [
            head,
            `  Где ещё есть:`,
            ...m.alternatives.map((a) => `  – ${a.label}: ${a.available} шт.`),
          ];
        }),
        ``,
        `Попробуйте выбрать другой склад, другой вкус или другую позицию.`,
      ].join("\n");

      return res.status(409).json({
        ok: false,
        error: "OUT_OF_STOCK",
        message,
        targetLabel,
        missing,
      });
    }

    return res.json({
      ok: true,
      cartDraft: {
        items: repeatedItems,
        checkoutDeliveryType: orig.deliveryType || null,
        checkoutDeliveryMethod: orig.deliveryMethod || null,
        checkoutPickupPointId: orig.pickupPointId || null,
        arrivalTime: orig.deliveryType === "pickup" ? null : (orig.arrivalTime ?? null),
        courierAddress: orig.courierAddress ?? null,
        inpostData: orig.inpostData ?? {
          fullName: null,
          phone: null,
          email: null,
          city: null,
          lockerAddress: null,
        },
      },
    });
  } catch (e) {
    console.error("POST /orders/repeat error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.get("/orders", async (req, res) => {
  try {
    const telegramId = String(req.query.telegramId || "").trim();
    if (!telegramId) return res.status(400).json({ ok: false, error: "telegramId is required" });

    const orders = await Order.find({ userTelegramId: telegramId }).sort({ createdAt: -1 }).lean();
    return res.json({ ok: true, orders });
  } catch (e) {
    console.error("GET /orders error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.post("/orders/:id/apply-cashback", async (req, res) => {
  try {
    const { id } = req.params;
    const { telegramId, mode } = req.body || {};

    const safeMode = String(mode || "partial").trim().toLowerCase();
    if (!["partial", "full"].includes(safeMode)) {
      return res.status(400).json({ ok: false, error: "INVALID_CASHBACK_MODE" });
    }

    const order = await Order.findById(id);
    if (!order) {
      return res.status(404).json({ ok: false, error: "ORDER_NOT_FOUND" });
    }

    if (String(order.userTelegramId || "") !== String(telegramId || "")) {
      return res.status(403).json({ ok: false, error: "FORBIDDEN" });
    }

    if (String(order.status || "") === "canceled") {
      return res.status(400).json({ ok: false, error: "ORDER_CANCELED" });
    }

    if (
      String(order.payment?.status || "") === "checking" ||
      String(order.payment?.status || "") === "paid"
    ) {
      return res.status(400).json({ ok: false, error: "PAYMENT_ALREADY_SUBMITTED" });
    }

    const user = await User.findOne({ telegramId: String(telegramId || "") });
    user.cashbackLedger = Array.isArray(user.cashbackLedger) ? user.cashbackLedger : [];
    if (!user) {
      return res.status(404).json({ ok: false, error: "USER_NOT_FOUND" });
    }

    const orderTotal = Number(order.totalZl || 0);
    const cashbackBalance = Number(user.cashbackBalance || 0);

    if (cashbackBalance <= 0) {
      return res.status(400).json({ ok: false, error: "NO_CASHBACK_BALANCE" });
    }

    let cashbackAppliedZl = 0;
    let remainingToPayZl = orderTotal;
    let cashbackFullyPaid = false;

    if (safeMode === "full") {
      if (cashbackBalance < orderTotal) {
        return res.status(400).json({ ok: false, error: "INSUFFICIENT_CASHBACK_FOR_FULL_PAYMENT" });
      }

      cashbackAppliedZl = Number(orderTotal.toFixed(2));
      remainingToPayZl = 0;
      cashbackFullyPaid = true;
    } else {
      cashbackAppliedZl = Number(Math.min(cashbackBalance, orderTotal).toFixed(2));
      remainingToPayZl = Number((orderTotal - cashbackAppliedZl).toFixed(2));
      cashbackFullyPaid = remainingToPayZl <= 0;
    }

    user.cashbackBalance = Number((cashbackBalance - cashbackAppliedZl).toFixed(2));

    let cashbackLeftToDeduct = Number(cashbackAppliedZl || 0);

    const activeRows = [...user.cashbackLedger]
      .filter((row) => !row?.expiredAt && Number(row?.remainingZl || 0) > 0)
      .sort((a, b) => new Date(a.expiresAt).getTime() - new Date(b.expiresAt).getTime());

    for (const row of activeRows) {
      if (cashbackLeftToDeduct <= 0) break;

      const available = Math.max(0, Number(row.remainingZl || 0));
      if (available <= 0) continue;

      const used = Math.min(available, cashbackLeftToDeduct);

      row.remainingZl = Number((available - used).toFixed(2));
      cashbackLeftToDeduct = Number((cashbackLeftToDeduct - used).toFixed(2));
    }

    recalcUserCashbackBalanceFromLedger(user);

    await user.save();

    const prevPayment = order.payment?.toObject ? order.payment.toObject() : (order.payment || {});
    order.payment = {
      ...prevPayment,
      method: cashbackFullyPaid ? "cashback" : String(prevPayment?.method || ""),
      cashbackAppliedZl,
      cashbackRemainingToPayZl: remainingToPayZl,
      cashbackFullyPaid,
      cashbackAppliedAt: new Date(),
      cashbackRefundedAt: null,
      checkedAt: null,
      checkedByTelegramId: "",
      status: "unpaid",
    };

    await order.save();

    const freshOrder = await Order.findById(order._id).lean();

    return res.json({
      ok: true,
      order: freshOrder,
      cashbackBalance: Number(user.cashbackBalance || 0),
      cashbackAppliedZl,
      cashbackRemainingToPayZl: remainingToPayZl,
      cashbackFullyPaid,
    });
  } catch (e) {
    console.error("apply cashback error:", e);
    return res.status(500).json({ ok: false, error: "SERVER_ERROR" });
  }
});

app.post("/orders/:id/arrived-at-pickup", async (req, res) => {
  try {
    const telegramId = String(req.body?.telegramId || "").trim();
    const { id } = req.params;

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const order = await Order.findOne({ _id: id, userTelegramId: telegramId });
    if (!order) {
      return res.status(404).json({ ok: false, error: "Order not found" });
    }

    if (String(order.deliveryType || "") !== "pickup") {
      return res.status(400).json({ ok: false, error: "Only pickup orders are supported" });
    }

    if (String(order.status || "") === "completed") {
      return res.json({ ok: true, order, alreadyCompleted: true });
    }

    order.arrivedNotifiedAt = new Date();
    await order.save();

    await notifyManagerClientArrived(order);

    return res.json({ ok: true, order });
  } catch (e) {
    console.error("POST /orders/:id/arrived-at-pickup error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.post("/orders/:id/payment-check", async (req, res) => {
  try {
    const telegramId = String(req.body?.telegramId || "").trim();
    const { id } = req.params;

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    const order = await Order.findOne({ _id: id, userTelegramId: telegramId });
    if (!order) {
      return res.status(404).json({ ok: false, error: "Order not found" });
    }

    const point = await resolveOrderPaymentPoint(order);

    const allowedMethods = Array.isArray(point?.paymentConfig?.methods)
      ? point.paymentConfig.methods
          .filter((m) => m && m.isActive !== false)
          .map((m) => String(m.key || "").trim())
          .filter(Boolean)
      : [];

    const requestedMethod = req.body?.paymentMethod
      ? String(req.body.paymentMethod).trim()
      : "";

    const cashbackFullyPaid = Boolean(order?.payment?.cashbackFullyPaid);

    if (!requestedMethod && !cashbackFullyPaid) {
      return res.status(400).json({ ok: false, error: "paymentMethod is required" });
    }

    if (requestedMethod && requestedMethod !== "cashback" && allowedMethods.length && !allowedMethods.includes(requestedMethod)) {
      return res.status(400).json({
        ok: false,
        error: "Payment method is not available for this order",
      });
    }

    if (String(order?.payment?.status || "") === "paid") {
      return res.json({ ok: true, order });
    }

    const prevPayment = order.payment?.toObject
      ? order.payment.toObject()
      : (order.payment || {});

    const cashbackAppliedZl = Number(prevPayment.cashbackAppliedZl || 0);
    const cashbackRemainingToPayZl = Number(prevPayment.cashbackRemainingToPayZl || 0);
    const managerDisplayAmount = Number(req.body?.managerDisplayAmount || 0);
    const managerDisplayCurrency = String(req.body?.managerDisplayCurrency || "PLN").trim() || "PLN";
    const managerDisplayRate =
      req.body?.managerDisplayRate === null ||
      req.body?.managerDisplayRate === undefined ||
      req.body?.managerDisplayRate === ""
        ? null
        : Number(req.body.managerDisplayRate || 0);

    order.payment = {
      ...(order.payment?.toObject ? order.payment.toObject() : order.payment || {}),
      status: "checking",
      method: cashbackFullyPaid
        ? "cashback"
        : (req.body?.paymentMethod
            ? String(req.body.paymentMethod)
            : (order.payment?.method || null)),
      cashChangeType: cashbackFullyPaid
        ? null
        : (req.body?.cashChangeType
            ? String(req.body.cashChangeType)
            : null),
      cashAmount: cashbackFullyPaid
        ? null
        : (req.body?.cashAmount
            ? String(req.body.cashAmount)
            : null),
      cashbackAppliedZl:
        cashbackAppliedZl !== undefined
          ? Number(Number(cashbackAppliedZl || 0).toFixed(2))
          : Number(Number(order.payment?.cashbackAppliedZl || 0).toFixed(2)),
      cashbackRemainingToPayZl:
        cashbackRemainingToPayZl !== undefined
          ? Number(Number(cashbackRemainingToPayZl || 0).toFixed(2))
          : Number(Number(order.payment?.cashbackRemainingToPayZl || 0).toFixed(2)),
      managerDisplayAmount,
      managerDisplayCurrency,
      managerDisplayRate,
      cashbackFullyPaid,
      checkedAt: new Date(),
      checkedByTelegramId: "",
    };

    await order.save();
    stopPaymentReminder(order._id);
    await sendOrderCreatedNotification(order);
    return res.json({ ok: true, order });
  } catch (e) {
    console.error("POST /orders/:id/payment-check error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

app.patch("/admin/orders/:id/payment-status", requireAdmin, async (req, res) => {
  try {
    const { id } = req.params;
    const status = String(req.body?.status || "").trim();
    const checkedByTelegramId = String(req.body?.checkedByTelegramId || "").trim();

    if (!["paid", "unpaid"].includes(status)) {
      return res.status(400).json({ ok: false, error: "status must be paid or unpaid" });
    }

    const order = await Order.findById(id);
    if (!order) {
      return res.status(404).json({ ok: false, error: "Order not found" });
    }

    order.payment = {
      ...(order.payment?.toObject ? order.payment.toObject() : order.payment || {}),
      status,
      paidAt: status === "paid" ? new Date() : null,
      checkedAt: new Date(),
      checkedByTelegramId,
    };

    await order.save();
    return res.json({ ok: true, order });
  } catch (e) {
    console.error("PATCH /admin/orders/:id/payment-status error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});

// ===== Admin: создать товар ====

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

    const rawFlavorKey = String(b.flavorKey || "").trim().toLowerCase();
    const label = String(b.label || "").trim();

    if (!label) {
      return res.status(400).json({ ok: false, error: "label is required" });
    }

    const gradient = Array.isArray(b.gradient) ? b.gradient.map((x) => String(x)) : [];
    if (gradient.length !== 2) {
      return res.status(400).json({ ok: false, error: "gradient must contain exactly 2 colors" });
    }

    const product = await Product.findById(id);
    if (!product) return res.status(404).json({ ok: false, error: "Product not found" });

    const requestedFlavorKey = rawFlavorKey || slugifyFlavorLabel(label);

    const existing = (product.flavors || []).find(
      (f) => String(f.flavorKey || "").trim().toLowerCase() === requestedFlavorKey
    );

    const sameFlavorByLabel = (product.flavors || []).find(
      (f) => String(f.label || "").trim().toLowerCase() === label.toLowerCase()
    );

    if (existing && sameFlavorByLabel && String(existing._id) === String(sameFlavorByLabel._id)) {
      // обновляем только если это реально тот же вкус
      existing.label = label;
      existing.gradient = gradient;
      if (b.isActive !== undefined) existing.isActive = !!b.isActive;
    } else {
      const uniqueFlavorKey = ensureUniqueFlavorKeyForProduct(product, requestedFlavorKey);

      product.flavors.push({
        flavorKey: uniqueFlavorKey,
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

app.get("/orders/:id/payment-config", async (req, res) => {
  try {
    const telegramId = String(req.query?.telegramId || "").trim();
    const orderId = String(req.params?.id || "").trim();

    if (!telegramId) {
      return res.status(400).json({ ok: false, error: "telegramId is required" });
    }

    if (!orderId) {
      return res.status(400).json({ ok: false, error: "order id is required" });
    }

    const order = await Order.findOne(
      { _id: orderId, userTelegramId: telegramId },
      {
        _id: 1,
        orderNo: 1,
        totalZl: 1,
        currency: 1,
        deliveryType: 1,
        deliveryMethod: 1,
        pickupPointId: 1,
        status: 1,
        payment: 1,
      }
    ).lean();

    if (!order) {
      return res.status(404).json({ ok: false, error: "Order not found" });
    }

    const point = await resolveOrderPaymentPoint(order);

    const methods = Array.isArray(point?.paymentConfig?.methods)
      ? point.paymentConfig.methods
          .filter((m) => m && m.isActive !== false && String(m.key || "").trim())
          .map((m) => ({
            key: String(m.key || "").trim(),
            label: String(m.label || "").trim(),
            detailsValue: String(m.detailsValue || "").trim(),
            badge: String(m.badge || "").trim(),
          }))
      : [];

    return res.json({
      ok: true,
      paymentConfig: {
        pointId: point?._id || null,
        pointTitle: point?.title || "",
        pointAddress: point?.address || "",
        methods,
      },
    });
  } catch (e) {
    console.error("GET /orders/:id/payment-config error:", e);
    return res.status(500).json({ ok: false, error: "Server error" });
  }
});


// ==== Telegram бот ====

const TG_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const WEBAPP_URL = process.env.WEBAPP_URL || "";
const START_BANNER_URL = String(process.env.START_BANNER_URL || "").trim();

if (TG_BOT_TOKEN) {
  bot = new Telegraf(TG_BOT_TOKEN);

  bot.start(async (ctx) => {
    try {
      const payload = String(ctx.startPayload || "").trim();
      const tgId = String(ctx.from?.id || "").trim();
      const username = String(ctx.from?.username || "").trim() || null;
      const firstName = String(ctx.from?.first_name || "").trim() || null;
      const lastName = String(ctx.from?.last_name || "").trim() || null;

      if (!tgId) {
        throw new Error("TG_ID_MISSING");
      }

      let me = await User.findOne({ telegramId: tgId });

      if (!me) {
        me = await User.create({
          telegramId: tgId,
          username,
          firstName,
          lastName,
          cashbackBalance: 0,
          cashbackLedger: [],
          referral: {
            code: "",
            usedCode: "",
            rewardGroups: [],
          },
        });
      } else {
        let changed = false;

        if (me.username !== username) {
          me.username = username;
          changed = true;
        }

        if (me.firstName !== firstName) {
          me.firstName = firstName;
          changed = true;
        }

        if (me.lastName !== lastName) {
          me.lastName = lastName;
          changed = true;
        }

        if (!me.referral || typeof me.referral !== "object") {
          me.referral = {
            code: "",
            usedCode: "",
            rewardGroups: [],
          };
          changed = true;
        }

        if (!Array.isArray(me.referral.rewardGroups)) {
          me.referral.rewardGroups = [];
          changed = true;
        }

        if (changed) {
          await me.save();
        }
      }

      let myRefCode = String(me?.referral?.code || "").trim();

      if (!myRefCode) {
        if (typeof ensureUserRefCode === "function") {
          myRefCode = await ensureUserRefCode(me);
        } else {
          myRefCode = genRefCode();
          me.referral = me.referral || {};
          me.referral.code = myRefCode;
          if (!Array.isArray(me.referral.rewardGroups)) {
            me.referral.rewardGroups = [];
          }
          await me.save();
        }
      }

      let openLink = String(WEBAPP_URL || "").trim();
      if (!openLink) {
        throw new Error("WEBAPP_URL_MISSING");
      }

      try {
        const u = new URL(openLink);
        if (payload) u.searchParams.set("startapp", payload);
        if (myRefCode) u.searchParams.set("ref", myRefCode);
        openLink = u.toString();
      } catch {
        const params = new URLSearchParams();
        if (payload) params.set("startapp", payload);
        if (myRefCode) params.set("ref", myRefCode);
        openLink = `${String(WEBAPP_URL || "").trim()}${params.toString() ? "?" + params.toString() : ""}`;
      }

      const caption = "Добро пожаловать в ELF DUCK SHOP!";
      const keyboard = Markup.inlineKeyboard([
        [Markup.button.webApp("💨 Посетить магазин 🛍️", openLink)],
      ]);

      if (START_BANNER_URL) {
        try {
          await ctx.replyWithPhoto(START_BANNER_URL, { caption, ...keyboard });
          return;
        } catch (photoErr) {
          console.error("[BOT_START] replyWithPhoto failed:", photoErr);
        }
      }

      await ctx.reply(caption, keyboard);
    } catch (e) {
      console.error("bot.start error:", e);
      try {
        await ctx.reply("Произошла ошибка при открытии магазина. Попробуйте ещё раз.");
      } catch {}
    }
  });

  bot.action(/mgr_pay_paid:(.+)/, async (ctx) => {
    try {
      const orderId = String(ctx.match?.[1] || "").trim();
      if (!orderId) return ctx.answerCbQuery("Order not found");

      const order = await Order.findById(orderId);
      if (!order) return ctx.answerCbQuery("Заказ не найден");

      // списываем склад только один раз
      if (!order.stockCommittedAt) {
        await commitOrderStock(order);
        order.stockCommittedAt = new Date();
      }

      const shouldMarkAwaiting =
      String(order?.deliveryType || "") === "pickup" &&
      String(order?.payment?.method || "") === "cash";

      order.payment = {
        ...(order.payment?.toObject ? order.payment.toObject() : order.payment || {}),
        status: shouldMarkAwaiting ? "awaiting" : "paid",
        paidAt: new Date(),
        checkedAt: new Date(),
        checkedByTelegramId: String(ctx.from?.id || ""),
      };

      if (
        String(order?.deliveryType || "") === "delivery" &&
        String(order?.deliveryMethod || "") === "courier"
      ) {
        order.courierUsername = String(ctx.from?.username || "").trim();
        order.courierTelegramId = String(ctx.from?.id || "").trim();
      }

      // После подтверждения оплаты заказ остается "assembled"
      // и только потом отдельно отмечается как shipped/completed.
      order.status = "assembled";
      // --- PATCH 1: replace block ---
      await order.save();

      await applyOrderCashback(order);

      const freshPaidOrder = await Order.findById(order._id);
      if (!freshPaidOrder) {
        throw new Error("ORDER_NOT_FOUND_AFTER_PAY");
      }

      await refreshManagerOrderMessage(freshPaidOrder);
      stopPaymentReminder(order._id);
      // --- END PATCH 1 ---

      // Для доставки отправляем отдельное сообщение-напоминание менеджеру
      if (String(order?.deliveryType || "") === "delivery") {
        try {
          const managerChatId = String(order?.payment?.managerMessageChatId || "").trim();
          const managerMessageId = Number(order?.payment?.managerMessageId || 0);
          const orderNo = escapeHtml(order?.orderNo || "—");
          const isInpost = String(order?.deliveryMethod || "").trim() === "inpost";

          const deliveryTitle = isInpost
            ? `📦 <b>ЗАКАЗ ГОТОВ К ОТПРАВКЕ</b>`
            : `🚚 <b>ЗАКАЗ ГОТОВ К ДОСТАВКЕ</b>`;

          // const courierUsernameRaw = String(order?.courierUsername || order?.courier?.username || "").trim();
          // const courierUsername = courierUsernameRaw
          //   ? (courierUsernameRaw.startsWith("@") ? courierUsernameRaw : `@${courierUsernameRaw}`)
          //   : "—";

          const deliveryText = isInpost
            ? `Когда вы отправите с помощью пачкомата этот заказ (<b>#${orderNo}</b>) нажмите кнопку <b>ЗАКАЗ ОТПРАВЛЕН</b>, чтобы клиент был уведомлен.`
            : `Когда вы прибудете на адрес по заказу <b>#${orderNo}</b>, нажмите кнопку <b>ЗАКАЗ ДОСТАВЛЕН</b>, чтобы клиент был уведомлен.`;

          const deliveryButton = isInpost
            ? { text: "📦 ЗАКАЗ ОТПРАВЛЕН", callback_data: `mgr_order_shipped:${order._id}` }
            : { text: "🚚 ЗАКАЗ ДОСТАВЛЕН", callback_data: `mgr_order_delivered:${order._id}` };

          if (managerChatId && managerMessageId) {
            const sent = await bot.telegram.sendMessage(
              managerChatId,
              [
                deliveryTitle,
                ``,
                deliveryText,
              ].join("\n"),
              {
                parse_mode: "HTML",
                reply_to_message_id: managerMessageId,
                allow_sending_without_reply: true,
                reply_markup: {
                  inline_keyboard: [[deliveryButton]],
                },
              }
            );

            await Order.updateOne(
              { _id: order._id },
              {
                $push: {
                  managerDeliveryMessageIds: String(sent?.message_id || ""),
                },
              }
            );
          }
        } catch (e) {
          console.error("mgr_pay_paid delivery notify error:", e);
        }
      }

      await ctx.answerCbQuery(
        shouldMarkAwaiting ? "Клиент ожидается на точке" : "Оплата подтверждена"
      );
      // --- PATCH 2: add collapse/collapseButton logic after answerCbQuery ---
try {
  await ctx.editMessageReplyMarkup({
    inline_keyboard: [
      [{ text: shouldMarkAwaiting ? "🕒 Ожидаю" : "✅ Оплачено", callback_data: `mgr_done:${freshPaidOrder._id}` }],
    ],
  });
} catch (e) {
  const msg = String(e?.response?.description || e?.message || "").toLowerCase();

  if (msg.includes("message is not modified")) {
    return;
  }

  console.error("mgr_pay_paid editMessageReplyMarkup error:", e);
}
      // --- END PATCH 2 ---
    } catch (e) {
      console.error("mgr_pay_paid error:", e);
      try {
        await ctx.answerCbQuery("Ошибка");
      } catch {}
    }
  });

  bot.action(/mgr_pay_unpaid:(.+)/, async (ctx) => {
    try {
      const orderId = String(ctx.match?.[1] || "").trim();
      if (!orderId) return ctx.answerCbQuery("Order not found");

      const order = await Order.findById(orderId);
      if (!order) return ctx.answerCbQuery("Заказ не найден");

      // снимаем резерв только один раз
      if (!order.stockReleasedAt) {
        await releaseOrderReservedStock(order);
        order.stockReleasedAt = new Date();
      }

      // возвращаем кэшбек, если он был применён
      await refundOrderCashback(order);

      const freshOrderAfterRefund = await Order.findById(order._id);
      if (!freshOrderAfterRefund) {
        throw new Error("ORDER_NOT_FOUND_AFTER_REFUND");
      }

      order.payment = {
        ...(freshOrderAfterRefund.payment?.toObject
          ? freshOrderAfterRefund.payment.toObject()
          : freshOrderAfterRefund.payment || {}),
        status: "unpaid",
        paidAt: null,
        checkedAt: new Date(),
        checkedByTelegramId: String(ctx.from?.id || ""),
      };

      order.status = "canceled";

      // --- PATCH 3: replace block for unpaid status ---
      await order.save();

      const freshUnpaidOrder = await Order.findById(order._id);
      if (!freshUnpaidOrder) {
        throw new Error("ORDER_NOT_FOUND_AFTER_UNPAID");
      }

      await refreshManagerOrderMessage(freshUnpaidOrder);

      stopPaymentReminder(order._id);

      await ctx.answerCbQuery("Оплата отклонена, кэшбек возвращён");

      try {
        await ctx.editMessageReplyMarkup({
          inline_keyboard: [
            [{ text: "❌ Оплата отклонена", callback_data: `mgr_done:${freshUnpaidOrder._id}` }],
          ],
        });
      } catch (e) {
        console.error("mgr_pay_unpaid editMessageReplyMarkup error:", e);
      }
      // --- END PATCH 3 ---
    } catch (e) {
      console.error("mgr_pay_unpaid error:", e);
      try {
        await ctx.answerCbQuery("Ошибка");
      } catch {}
    }
  });

  bot.action(/mgr_order_shipped:(.+)/, async (ctx) => {
    try {
      const orderId = String(ctx.match?.[1] || "").trim();
      if (!orderId) {
        await ctx.answerCbQuery("Заказ не найден");
        return;
      }

      const order = await Order.findById(orderId);
      if (!order) {
        await ctx.answerCbQuery("Заказ не найден");
        return;
      }

      if (String(order.status || "") === "shipped") {
        await ctx.answerCbQuery("Заказ уже отправлен");
        return;
      }

      order.status = "shipped";
      order.shippedAt = new Date();
      await order.save();

      const deliveryMessageIds = Array.isArray(order.managerDeliveryMessageIds)
        ? order.managerDeliveryMessageIds.filter(Boolean)
        : [];

      const deliveryChatId = String(order?.payment?.managerMessageChatId || "").trim();

      for (const messageId of deliveryMessageIds) {
        try {
          if (deliveryChatId && messageId) {
            await bot.telegram.deleteMessage(deliveryChatId, Number(messageId));
          }
        } catch (_) {}
      }

      if (deliveryMessageIds.length) {
        order.managerDeliveryMessageIds = [];
        await order.save();
      }

      const freshShippedOrder = await Order.findById(order._id);
      if (!freshShippedOrder) {
        throw new Error("ORDER_NOT_FOUND_AFTER_SHIPPED");
      }

      await refreshManagerOrderMessage(freshShippedOrder);

      try {
        const mainChatId = String(freshShippedOrder?.payment?.managerMessageChatId || "").trim();
        const mainMessageId = Number(freshShippedOrder?.payment?.managerMessageId || 0);

        if (mainChatId && mainMessageId) {
          await bot.telegram.editMessageReplyMarkup(mainChatId, mainMessageId, undefined, {
            inline_keyboard: [
              [{ text: "📦 Заказ отправлен", callback_data: `mgr_order_shipped_done:${freshShippedOrder._id}` }],
            ],
          });
        }
      } catch (e) {
        console.error("mgr_order_shipped main message markup error:", e);
      }

      try {
        if (bot && order?.userTelegramId) {
          const orderNo = escapeHtml(order?.orderNo || "—");

          await bot.telegram.sendMessage(
            String(order.userTelegramId),
            [
              `📦 <b>ЗАКАЗ ОТПРАВЛЕН</b>`,
              ``,
              `Твой заказ <b>#${orderNo}</b> отправлен через InPost.`,
            ].join("\n"),
            {
              parse_mode: "HTML",
              disable_web_page_preview: true,
            }
          );
        }
      } catch (e) {
        console.error("mgr_order_shipped client notify error:", e);
      }

      await ctx.answerCbQuery("Заказ отмечен как отправленный");

      try {
        await ctx.deleteMessage();
      } catch (_) {}
    } catch (e) {
      console.error("mgr_order_shipped error:", e);
      try {
        await ctx.answerCbQuery("Не удалось отметить заказ как отправленный");
      } catch {}
    }
  });

  bot.action(/mgr_order_delivered:(.+)/, async (ctx) => {
    try {
      const orderId = String(ctx.match?.[1] || "").trim();
      if (!orderId) {
        await ctx.answerCbQuery("Заказ не найден");
        return;
      }

      const order = await Order.findById(orderId);
      if (!order) {
        await ctx.answerCbQuery("Заказ не найден");
        return;
      }

      if (String(order.status || "") === "completed") {
        await ctx.answerCbQuery("Заказ уже доставлен");
        return;
      }

      order.status = "completed";
      order.completedAt = new Date();
      await order.save();

      const deliveryMessageIds = Array.isArray(order.managerDeliveryMessageIds)
        ? order.managerDeliveryMessageIds.filter(Boolean)
        : [];

      const deliveryChatId = String(order?.payment?.managerMessageChatId || "").trim();

      for (const messageId of deliveryMessageIds) {
        try {
          if (deliveryChatId && messageId) {
            await bot.telegram.deleteMessage(deliveryChatId, Number(messageId));
          }
        } catch (_) {}
      }

      if (deliveryMessageIds.length) {
        order.managerDeliveryMessageIds = [];
        await order.save();
      }

      const freshDeliveredOrder = await Order.findById(order._id);
      if (!freshDeliveredOrder) {
        throw new Error("ORDER_NOT_FOUND_AFTER_DELIVERED");
      }

      await refreshManagerOrderMessage(freshDeliveredOrder);

      try {
        const mainChatId = String(freshDeliveredOrder?.payment?.managerMessageChatId || "").trim();
        const mainMessageId = Number(freshDeliveredOrder?.payment?.managerMessageId || 0);

        if (mainChatId && mainMessageId) {
          await bot.telegram.editMessageReplyMarkup(mainChatId, mainMessageId, undefined, {
            inline_keyboard: [
              [{ text: "🚚 Заказ доставлен", callback_data: `mgr_order_completed_done:${freshDeliveredOrder._id}` }],
            ],
          });
        }
      } catch (e) {
        const msg = String(e?.response?.description || e?.message || "").toLowerCase();

        if (!msg.includes("message is not modified")) {
          console.error("mgr_order_delivered main message markup error:", e);
        }
      }

    try {
      const safeTelegramId = String(order?.userTelegramId || "").trim();

      if (bot && safeTelegramId) {

        const orderNo = escapeHtml(order?.orderNo || "—");
        const notifyPoint = await resolveOrderNotificationPoint(freshDeliveredOrder || order).catch(() => null);

        const pointManagerTelegramId = String(
          Array.isArray(notifyPoint?.allowedAdminTelegramIds)
            ? notifyPoint.allowedAdminTelegramIds[0] || ""
            : ""
        ).trim();

        const pointManagerUser = pointManagerTelegramId
          ? await User.findOne(
              { telegramId: pointManagerTelegramId },
              { telegramId: 1, username: 1, firstName: 1 }
            ).lean()
          : null;

        const courierUsernameRaw = String(
          pointManagerUser?.username ||
          notifyPoint?.managerUsername ||
          notifyPoint?.courierUsername ||
          freshDeliveredOrder?.courierUsername ||
          freshDeliveredOrder?.courier?.username ||
          order?.courierUsername ||
          order?.courier?.username ||
          ""
        ).trim();

        const courierUsername = courierUsernameRaw
          ? (courierUsernameRaw.startsWith("@")
              ? courierUsernameRaw
              : `@${courierUsernameRaw}`)
          : "—";

          await bot.telegram.sendMessage(
            safeTelegramId,
            [
              `🚚 <b>КУРЬЕР ПРИБЫЛ НА АДРЕС</b>`,
              ``,
              `Курьер прибыл по заказу <b>#${orderNo}</b>.`,
              ``,
              `📲 <b>Связь с курьером:</b> ${escapeHtml(courierUsername)}`,
            ].join("\n"),
            {
              parse_mode: "HTML",
              disable_web_page_preview: true,
            }
          );
        } else {
          console.warn("mgr_order_delivered client notify skipped:", {
            hasBot: Boolean(bot),
            safeTelegramId,
            orderId: String(order?._id || ""),
            orderNo: String(order?.orderNo || ""),
          });
        }
      } catch (e) {
        console.error("mgr_order_delivered client notify error:", {
          orderId: String(order?._id || ""),
          orderNo: String(order?.orderNo || ""),
          userTelegramId: String(order?.userTelegramId || ""),
          error: e?.response?.description || e?.message || String(e),
        });
      }

      await ctx.answerCbQuery("Клиент уведомлен о прибытии курьера");

      try {
        await ctx.deleteMessage();
      } catch (_) {}
    } catch (e) {
      console.error("mgr_order_delivered error:", e);
      try {
        await ctx.answerCbQuery("Не удалось отметить заказ как доставленный");
      } catch {}
    }
  });

  bot.action(/mgr_order_completed:(.+)/, async (ctx) => {
    try {
      const orderId = String(ctx.match?.[1] || "").trim();
      if (!orderId) {
        await ctx.answerCbQuery("Заказ не найден");
        return;
      }

      const order = await Order.findById(orderId);
      if (!order) {
        await ctx.answerCbQuery("Заказ не найден");
        return;
      }

      if (String(order.status || "") === "completed") {
        await ctx.answerCbQuery("Заказ уже выполнен");
        return;
      }

      order.status = "completed";
      order.completedAt = new Date();
      await order.save();

      const arrivalMessageIds = Array.isArray(order.managerArrivalMessageIds)
        ? order.managerArrivalMessageIds.filter(Boolean)
        : [];

      const arrivalChatId = String(order?.payment?.managerMessageChatId || "").trim();

      for (const messageId of arrivalMessageIds) {
        try {
          if (arrivalChatId && messageId) {
            await bot.telegram.deleteMessage(arrivalChatId, Number(messageId));
          }
        } catch (_) {}
      }

      if (arrivalMessageIds.length) {
        order.managerArrivalMessageIds = [];
        await order.save();
      }

      await refreshManagerOrderMessage(order);
      await ctx.answerCbQuery("Заказ отмечен как выполненный");

      try {
        await ctx.deleteMessage();
      } catch (_) {}
    } catch (e) {
      console.error("mgr_order_completed error:", e);
      await ctx.answerCbQuery("Не удалось завершить заказ");
    }
  });

  bot.action(/mgr_done:(.+)/, async (ctx) => {
    try {
      await ctx.answerCbQuery("Статус уже обновлён");
    } catch {}
  });

  bot.launch()
    .then(() => {
      console.log("✅ User bot launched");
    })
    .catch((e) => {
      console.error("❌ bot.launch error:", e);
    });
    
  process.once("SIGINT", () => {
    try {
      bot?.stop("SIGINT");
    } catch {}
  });

  process.once("SIGTERM", () => {
    try {
      bot?.stop("SIGTERM");
    } catch {}
  });

} else {
  console.warn("⚠️ TELEGRAM_BOT_TOKEN not set — bot disabled");
}

// старт сервера
const PORT = process.env.PORT || 8080;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server running on port ${PORT}`);
});

setInterval(() => {
  processCashbackLedgerExpirations().catch((e) => {
    console.error("cashback expiration interval error:", e);
  });
}, 6 * 60 * 60 * 1000);

setInterval(() => {
  processOrdersWithoutPaymentConfirm();
}, 60 * 1000);

processOrdersWithoutPaymentConfirm();

processCashbackLedgerExpirations().catch((e) => {
  console.error("initial cashback expiration run error:", e);
});