import mongoose from "mongoose";

const categorySchema = new mongoose.Schema(
  {
    // уникальный ключ категории (используем в product.categoryKey)
    key: { type: String, required: true, unique: true }, // "liquids", "pods"

    // заголовок карточки
    title: { type: String, required: true }, // "ЖИДКОСТИ"

    // включать/скрывать категорию в каталоге
    isActive: { type: Boolean, default: true },

    // ---- UI параметры карточки (все редактируемые) ----
    // фон-картинка карточки
    cardBgUrl: { type: String, default: "" },

    // персонаж/утка на карточке
    cardDuckUrl: { type: String, default: "" },

    // класс позиционирования персонажа (cardImageLeft, cardImageRight, cardImageLeft2, ...)
    classCardDuck: { type: String, default: "" },

    // класс заголовка (cardTitle / cardTitle2) — если нужно как у тебя
    titleClass: { type: String, default: "cardTitle" },

    // показывать затемнение поверх (cardOverlay)
    showOverlay: { type: Boolean, default: false },

    // текст бейджа (например "NEW DROP")
    badgeText: { type: String, default: "" },

    badgeSide: { type: String, enum: ["left", "right"], default: "left" },

    // сортировка в сетке категорий
    sortOrder: { type: Number, default: 0 },
  },
  { timestamps: true }
);

export default mongoose.model("Category", categorySchema);