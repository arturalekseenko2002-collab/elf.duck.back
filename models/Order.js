import mongoose from "mongoose";

const orderItemFlavorSchema = new mongoose.Schema(
  {
    flavorKey: { type: String, required: true },
    qty: { type: Number, required: true, min: 1 },
  },
  { _id: false }
);

const orderItemSchema = new mongoose.Schema(
  {
    productId: { type: mongoose.Schema.Types.ObjectId, ref: "Product", required: true },
    productKey: { type: String, default: "" }, // удобно для отчетов/логов
    flavors: { type: [orderItemFlavorSchema], default: [] },
  },
  { _id: false }
);

const orderSchema = new mongoose.Schema(
  {
    // кто оформил
    userTelegramId: { type: String, default: "" },

    // доставка/самовывоз
    deliveryType: { type: String, enum: ["delivery", "pickup"], default: "delivery" },

    // точка самовывоза (если pickup)
    pickupPointId: { type: mongoose.Schema.Types.ObjectId, ref: "PickupPoint", default: null },

    // позиции заказа
    items: { type: [orderItemSchema], default: [] },

    // статусы
    status: {
      type: String,
      enum: ["created", "processing", "done", "canceled"],
      default: "created",
      index: true,
    },

    // чтобы резерв/списание было идемпотентным (1 раз)
    stockReservedAt: { type: Date, default: null },
    stockCommittedAt: { type: Date, default: null },
    stockReleasedAt: { type: Date, default: null },

    // кто обработал (менеджер)
    handledByTelegramId: { type: String, default: "" },
  },
  { timestamps: true }
);

export default mongoose.models.Order || mongoose.model("Order", orderSchema);