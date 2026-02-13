import mongoose from "mongoose";

/* ================= STOCK PER MANAGER ================= */
const stockByManagerSchema = new mongoose.Schema(
  {
    managerTelegramId: { type: String, required: true },
    qty: { type: Number, default: 0 },

    updatedByTelegramId: { type: String, default: "" },
    updatedAt: { type: Date, default: Date.now },
  },
  { _id: false }
);

/* ================= FLAVOR ================= */
const flavorSchema = new mongoose.Schema(
  {
    flavorKey: { type: String, required: true },
    label: { type: String, required: true },
    isActive: { type: Boolean, default: true },

    gradient: {
      type: [String],
      default: [],
      validate: {
        validator: (arr) => Array.isArray(arr) && arr.length === 2,
        message: "gradient must contain exactly 2 colors",
      },
    },

    stockByManager: { type: [stockByManagerSchema], default: [] },
  },
  { timestamps: false }
);

/* ================= PRODUCT ================= */
const productSchema = new mongoose.Schema(
  {
    productKey: { type: String, required: true, unique: true },

    // 🔥 category is dynamic (created from admin/front). Store by key.
    categoryKey: { type: String, required: true },

    isActive: { type: Boolean, default: true },

    title1: { type: String, default: "" },
    title2: { type: String, default: "" },
    titleModal: { type: String, default: "" },
    price: { type: Number, default: 0 },

    // media URLs
    cardBgUrl: { type: String, default: "" },
    cardDuckUrl: { type: String, default: "" },
    orderImgUrl: { type: String, default: "" },

    // UI configuration
    classCardDuck: { type: String, default: "" },
    classActions: { type: String, default: "" },
    classNewBadge: { type: String, default: "" },
    newBadge: { type: String, default: "" },

    accentColor: { type: String, default: "" },

    flavors: { type: [flavorSchema], default: [] },
  },
  { timestamps: true }
);

export default mongoose.model("Product", productSchema);