import mongoose from "mongoose";

/* ================= CART ITEM ================= */
const cartItemSchema = new mongoose.Schema(
  {
    productKey: { type: String, required: true },
    flavorKey: { type: String, required: true },

    qty: { type: Number, required: true, min: 1, default: 1 },

    // фиксируем цену за 1 шт на момент добавления в корзину
    unitPrice: { type: Number, default: 0, min: 0 },

    // точка для позиции (пока может быть null)
    pickupPointId: { type: mongoose.Schema.Types.ObjectId, ref: "PickupPoint", default: null },

    // UI
    flavorLabel: { type: String, default: "" },
    gradient: { type: [String], default: [] },
  },
  { _id: false }
);

/* ================= CART ================= */
const cartSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true, index: true },
    items: { type: [cartItemSchema], default: [] },

    // “сохраненная” точка на оформлении (одна на весь заказ)
    checkoutPickupPointId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "PickupPoint",
      default: null,
    },
  },
  { timestamps: true }
);

export default mongoose.models.Cart || mongoose.model("Cart", cartSchema);