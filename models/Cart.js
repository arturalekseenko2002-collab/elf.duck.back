import mongoose from "mongoose";

/* ================= CART ITEM ================= */
const cartItemSchema = new mongoose.Schema(
  {
    // чтобы можно было однозначно мерджить одинаковые позиции
    // (товар + вкус + точка)
    productKey: { type: String, required: true },
    flavorKey: { type: String, required: true },

    // количество выбранного вкуса
    qty: { type: Number, required: true, min: 1, default: 1 },

    // для UI / истории (не обязательное, но удобно)
    flavorLabel: { type: String, default: "" },
    gradient: { type: [String], default: [] }, // ["#..", "#.."]
  },
  { _id: false }
);

/* ================= CART ================= */
const cartSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true, index: true },

    // текущая корзина
    items: { type: [cartItemSchema], default: [] },

    title1: { type: String, default: "" },
    title2: { type: String, default: "" },
    unitPrice: { type: Number, default: 0 },

    cardBgUrl: { type: String, default: "" },
    cardDuckUrl: { type: String, default: "" },

    newBadge: { type: String, default: "" }, // "NEW"/"SALE"/"" 

    // “сохраненная” точка на оформлении (можно менять потом в корзине)
    checkoutPickupPointId: { type: mongoose.Schema.Types.ObjectId, ref: "PickupPoint", default: null },

    // TTL: удаляем корзины, которые не обновлялись 1 день
    updatedAt: { timestamps: true },
  },
  { timestamps: true }
);

export default mongoose.models.Cart || mongoose.model("Cart", cartSchema);