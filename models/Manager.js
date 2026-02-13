import mongoose from "mongoose";

const managerSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true }, // ← managerId
    username: String,
    firstName: String,
    lastName: String,

    pickupAddress: { type: String, default: "" }, // адрес точки
    isActive: { type: Boolean, default: true },
  },
  { timestamps: true }
);

export default mongoose.model("Manager", managerSchema);