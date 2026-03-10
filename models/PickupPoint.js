// ===== PickupPoint model (temporarily stored in Manager.js) =====
import mongoose from "mongoose";

const PickupPointSchema = new mongoose.Schema(
  {
    key: { type: String, required: true, unique: true, index: true }, // "krucza-03"
    title: { type: String, default: "" },
    address: { type: String, default: "" },
    sortOrder: { type: Number, default: 0 },
    isActive: { type: Boolean, default: true },

    paymentConfig: {
      methods: {
        type: [
          {
            key: { type: String, required: true }, // blik | crypto | ua_card | cash
            label: { type: String, default: "" },
            detailsValue: { type: String, default: "" },
            badge: { type: String, default: "" },
            isActive: { type: Boolean, default: true },
          },
        ],
        default: [],
      },
    },

    // telegramId админов/менеджеров, которым разрешено работать с этой точкой
    allowedAdminTelegramIds: { type: [String], default: [] },

    // 🔔 канал или группа для уведомлений
    notificationChatId: { type: String, default: "" },
  },
  { timestamps: true }
);

const PickupPoint =
  mongoose.models.PickupPoint || mongoose.model("PickupPoint", PickupPointSchema);

export default PickupPoint;