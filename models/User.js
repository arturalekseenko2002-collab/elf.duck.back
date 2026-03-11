import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true },
    username: String,
    firstName: String,
    lastName: String,
    photoUrl: String,

    cashbackBalance: { type: Number, default: 0 },

    favoriteProductKeys: { type: [String], default: [] },

    referral: {
      code: { type: String, default: "" },
      referredBy: { type: String, default: null },
      referredByCode: { type: String, default: null },
      referredAt: { type: Date, default: null },
      referralsCount: { type: Number, default: 0 },
      claimedPairsCount: { type: Number, default: 0 },
      referrals: {
        type: [
          {
            telegramId: { type: String, required: true },
            at: { type: Date, default: Date.now },
          },
        ],
        default: [],
      },
  }

  },
  { timestamps: true }
);

export default mongoose.model("User", userSchema);