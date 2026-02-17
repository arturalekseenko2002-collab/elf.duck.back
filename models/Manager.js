import mongoose from "mongoose";

// ===== Manager model =====
// Менеджер = админ, который работает с ассортиментом.
// telegramId используется как уникальный идентификатор.
const ManagerSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true, index: true },

    // удобно для UI/логов
    username: { type: String, default: "" },
    name: { type: String, default: "" },

    // закрепленная точка самовывоза (чтобы менеджер не выбирал её каждый раз)
    pickupPointId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "PickupPoint",
      default: null,
    },

    isActive: { type: Boolean, default: true },
  },
  { timestamps: true }
);

const Manager = mongoose.models.Manager || mongoose.model("Manager", ManagerSchema);

export default Manager;