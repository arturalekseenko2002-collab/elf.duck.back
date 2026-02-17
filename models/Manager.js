// ===== PickupPoint model (kept in server.js for now) =====
const PickupPointSchema = new mongoose.Schema(
  {
    key: { type: String, required: true, unique: true, index: true }, // "krucza-03"
    title: { type: String, default: "" },
    address: { type: String, default: "" },
    sortOrder: { type: Number, default: 0 },
    isActive: { type: Boolean, default: true },

    // telegramId админов/менеджеров, которым разрешено работать с этой точкой
    allowedAdminTelegramIds: { type: [String], default: [] },
  },
  { timestamps: true }
);

const PickupPoint =
  mongoose.models.PickupPoint || mongoose.model("PickupPoint", PickupPointSchema);