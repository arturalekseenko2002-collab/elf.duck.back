import mongoose from "mongoose";

/* ================= CART ITEM ================= */
const cartItemSchema = new mongoose.Schema(
  {
    // unique position key: (product + flavor)
    productKey: { type: String, required: true },
    flavorKey: { type: String, required: true },

    // quantity
    qty: { type: Number, required: true, min: 1, default: 1 },

    // IMPORTANT: price is stored per item (snapshot)
    unitPrice: { type: Number, default: 0, min: 0 },

    // UI helpers (optional)
    flavorLabel: { type: String, default: "" },
    gradient: { type: [String], default: [] }, // ["#..", "#.."]
  },
  { _id: false }
);

/* ================= CART ================= */
const cartSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true, index: true },

    // current cart items
    items: { type: [cartItemSchema], default: [] },

    // fixed (by first add) method of receiving goods
    checkoutDeliveryType: {
      type: String,
      enum: ["pickup", "delivery", null],
      default: null,
    },

    checkoutDeliveryMethod: {
      type: String,
      enum: ["courier", "inpost", null],
      default: null,
    },

    courierAddress: { type: String, default: null },

    inpostData: {
      fullName: { type: String, default: null },
      phone: { type: String, default: null },
      email: { type: String, default: null },
      city: { type: String, default: null },
      lockerAddress: { type: String, default: null },
    },

    // one order = one pickup point (can be changed later in cart/checkout)
    checkoutPickupPointId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "PickupPoint",
      default: null,
    },
  },
  { timestamps: true }
);

export default mongoose.models.Cart || mongoose.model("Cart", cartSchema);