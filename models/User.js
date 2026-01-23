import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true },
    username: String,
    firstName: String,
    lastName: String,
    photoUrl: String,

    referral: {
      code: String,       
      referredBy: String,  
      referredByCode: String,
      referredAt: Date,
      referralsCount: { type: Number, default: 0 },
      referrals: [
        {
          telegramId: String,
          at: Date,
        },
      ],
    },
  },
  { timestamps: true }
);

export default mongoose.model("User", userSchema);