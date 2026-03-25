import mongoose from "mongoose";

const orderItemFlavorSchema = new mongoose.Schema(
  {
    flavorKey: { type: String, required: true },
    qty: { type: Number, required: true, min: 1 },

    // snapshot из корзины
    unitPrice: { type: Number, default: 0, min: 0 },
    baseUnitPrice: { type: Number, default: 0, min: 0 },
    flavorLabel: { type: String, default: "" },
    gradient: { type: [String], default: [] },
  },
  { _id: false }
);

const orderItemSchema = new mongoose.Schema(
  {
    productId: { type: mongoose.Schema.Types.ObjectId, ref: "Product", required: true },
    productKey: { type: String, default: "" },

    // snapshot товара на момент покупки
    productTitle1: { type: String, default: "" },
    productTitle2: { type: String, default: "" },
    orderImgUrl: { type: String, default: "" },
    cardBgUrl: { type: String, default: "" },

    flavors: { type: [orderItemFlavorSchema], default: [] },
  },
  { _id: false }
);

const orderSchema = new mongoose.Schema(
  {
    // кто оформил
    userTelegramId: { type: String, required: true, index: true },

    // доставка/самовывоз
    deliveryType: {
      type: String,
      enum: ["delivery", "pickup"],
      default: null,
    },

    // точка самовывоза (если pickup)
    pickupPointId: { type: mongoose.Schema.Types.ObjectId, ref: "PickupPoint", default: null },

    // позиции заказа
    items: { 
      type: [orderItemSchema], 
      default: [], 
      required: true 
    },

    // статусы
    status: {
      type: String,
      enum: ["created", "processing", "assembled", "completed", "done", "shipped", "canceled"],
      default: "created",
      index: true,
    },

    stockReservedAt: { type: Date, default: null },
    stockCommittedAt: { type: Date, default: null },
    stockReleasedAt: { type: Date, default: null },

    // кто обработал (менеджер)
    handledByTelegramId: { type: String, default: "" },

    orderNo: { type: String, required: true, unique: true, index: true },

    totalZl: { type: Number, default: 0 },
    currency: { type: String, default: "PLN" },

    bgUrl: { type: String, default: "" },
    methodLabel: { type: String, default: "" },

    deliveryMethod: {
      type: String,
      enum: ["courier", "inpost"],
      default: null,
    },

    courierAddress: { type: String, default: null },
    courierUsername: { type: String, default: "" },
    courierTelegramId: { type: String, default: "" },
    inpostData: {
      fullName: { type: String, default: null },
      phone: { type: String, default: null },
      email: { type: String, default: null },
      city: { type: String, default: null },
      lockerAddress: { type: String, default: null },
    },

    arrivalTime: { type: String, default: null },

    payment: {
      status: { type: String, enum: ["unpaid", "checking", "paid", "refunded"], default: "unpaid" },
      method: { type: String, default: null },
      paidAt: { type: Date, default: null },
      amountZl: { type: Number, default: 0 },
      provider: { type: String, default: null },
      txId: { type: String, default: null },

      cashChangeType: { type: String, default: null },
      cashAmount: { type: String, default: null },

      checkedAt: { type: Date, default: null },
      checkedByTelegramId: { type: String, default: "" },

      cashbackAppliedZl: { type: Number, default: 0 },
      cashbackRemainingToPayZl: { type: Number, default: 0 },
      cashbackFullyPaid: { type: Boolean, default: false },
      cashbackAppliedAt: { type: Date, default: null },
      cashbackRefundedAt: { type: Date, default: null },

      managerMessageChatId: { type: String, default: "" },
      managerMessageId: { type: String, default: "" },
    },

    arrivedNotifiedAt: { type: Date, default: null }, // клиент нажал "Я на месте"
    managerArrivalMessageIds: { type: [String], default: [] },
    completedAt: { type: Date, default: null },       // менеджер отметил выполненным (опционально)

    cashbackPercent: { type: Number, default: 0 },
    cashbackZl: { type: Number, default: 0 },


  },
  { timestamps: true }
);

export default mongoose.models.Order || mongoose.model("Order", orderSchema);