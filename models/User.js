import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    telegramId: { type: String, required: true, unique: true },
    username: String,
    firstName: String,
    lastName: String,
    photoUrl: String,

    cashbackBalance: { type: Number, default: 0 },

    cashbackLedger: {
      type: [
        new mongoose.Schema(
          {
            sourceOrderId: { type: mongoose.Schema.Types.ObjectId, ref: "Order", default: null },
            amountZl: { type: Number, default: 0 },
            remainingZl: { type: Number, default: 0 },
            earnedAt: { type: Date, default: Date.now },
            expiresAt: { type: Date, default: null },
            warnedAt: { type: Date, default: null },
            expiredAt: { type: Date, default: null },
          },
          { _id: true }
        ),
      ],
      default: [],
    },

    favoriteProductKeys: { type: [String], default: [] },

    referral: {
      code: { type: String, default: "" },
      usedCode: { type: String, default: "" },
      invitedByTelegramId: { type: String, default: "" },
      firstOrderDoneAt: { type: Date, default: null },

      rewardGroups: {
        type: [
          new mongoose.Schema(
            {
              pairIndex: { type: Number, required: true },
              memberTelegramIds: { type: [String], default: [] },
              rewardClaimed: { type: Boolean, default: false },
              rewardClaimedAt: { type: Date, default: null },
              rewardAmountZl: { type: Number, default: 25 },
            },
            { _id: true }
          ),
        ],
        default: [],
      },
    },

  },
  { timestamps: true }
);

userSchema.index({ "referral.usedCode": 1 });

export default mongoose.model("User", userSchema);