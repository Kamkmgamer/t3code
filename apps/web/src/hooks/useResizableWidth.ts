import * as Schema from "effect/Schema";
import {
  type KeyboardEvent as ReactKeyboardEvent,
  type PointerEvent as ReactPointerEvent,
  useLayoutEffect,
  useRef,
  useState,
} from "react";

import { getLocalStorageItem, setLocalStorageItem } from "./useLocalStorage";
import { useResizeDrag } from "./useResizeDrag";

const WidthSchema = Schema.Finite;

/** The host must size itself from this custom property. */
export const RESIZABLE_WIDTH_PROPERTY = "--resizable-width";

export interface UseResizableWidthOptions {
  readonly storageKey: string;
  readonly defaultWidth: number;
  readonly minWidth: number;
  readonly maxWidth: number;
  readonly edge: "left" | "right";
}

export interface ResizableWidthHandlers {
  readonly onKeyDown: (event: ReactKeyboardEvent<HTMLElement>) => void;
  readonly onPointerDown: (event: ReactPointerEvent<HTMLElement>) => void;
  readonly onPointerMove: (event: ReactPointerEvent<HTMLElement>) => void;
  readonly onPointerUp: (event: ReactPointerEvent<HTMLElement>) => void;
  readonly onPointerCancel: (event: ReactPointerEvent<HTMLElement>) => void;
  readonly onLostPointerCapture: (event: ReactPointerEvent<HTMLElement>) => void;
}

const KEYBOARD_RESIZE_STEP = 16;
const KEYBOARD_RESIZE_LARGE_STEP = 64;

/** Persist on drag end to avoid a storage write on every animation frame. */
export function useResizableWidth(options: UseResizableWidthOptions): {
  readonly width: number;
  readonly minWidth: number;
  readonly maxWidth: number;
  readonly handlers: ResizableWidthHandlers;
} {
  const { storageKey, defaultWidth, minWidth, maxWidth, edge } = options;

  const clamp = (value: number): number => {
    if (!Number.isFinite(value)) return defaultWidth;
    return Math.max(minWidth, Math.min(maxWidth, value));
  };

  const readWidth = () => {
    if (typeof window === "undefined") return defaultWidth;
    try {
      const stored = getLocalStorageItem(storageKey, WidthSchema);
      return clamp(stored ?? defaultWidth);
    } catch (error) {
      console.error("Could not read persisted panel width.", error);
      return defaultWidth;
    }
  };
  const [widthState, setWidthState] = useState(() => ({ storageKey, width: readWidth() }));
  if (widthState.storageKey !== storageKey) {
    setWidthState({ storageKey, width: readWidth() });
  }

  const clampedWidth = clamp(widthState.width);
  const latestOptions = useRef({ clamp, storageKey, width: clampedWidth });
  useLayoutEffect(() => {
    latestOptions.current = { clamp, storageKey, width: clampedWidth };
  }, [clamp, clampedWidth, storageKey]);

  const { refresh, ...handlers } = useResizeDrag<HTMLElement>((event) => {
    const host = event.currentTarget.parentElement;
    // Disable drag transitions through transition-property; the host renders transition-duration.
    host?.style.setProperty("transition-property", "none");
    return {
      width: clampedWidth,
      edge,
      resize(value) {
        const nextWidth = latestOptions.current.clamp(value);
        host?.style.setProperty(RESIZABLE_WIDTH_PROPERTY, `${nextWidth}px`);
        return nextWidth;
      },
      finish(finalWidth, moved) {
        if (!moved) return;
        setWidthState({ storageKey, width: finalWidth });
        try {
          setLocalStorageItem(latestOptions.current.storageKey, finalWidth, WidthSchema);
        } catch (error) {
          console.error("Could not persist panel width.", error);
        }
      },
      cleanup(committed) {
        host?.style.removeProperty("transition-property");
        if (!committed) {
          host?.style.setProperty(RESIZABLE_WIDTH_PROPERTY, `${latestOptions.current.width}px`);
        }
      },
    };
  }, storageKey);

  // Mid-drag bound changes rewrite committed width; restore the live pointer position.
  useLayoutEffect(refresh, [clamp, clampedWidth, refresh]);

  const onKeyDown = (event: ReactKeyboardEvent<HTMLElement>) => {
    if (event.nativeEvent.isComposing || event.ctrlKey || event.altKey || event.metaKey) return;

    let nextWidth: number | undefined;
    if (event.key === "Home") {
      nextWidth = minWidth;
    } else if (event.key === "End" && Number.isFinite(maxWidth)) {
      nextWidth = maxWidth;
    } else if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
      const step = event.shiftKey ? KEYBOARD_RESIZE_LARGE_STEP : KEYBOARD_RESIZE_STEP;
      const direction =
        event.key === "ArrowLeft" ? (edge === "left" ? 1 : -1) : edge === "left" ? -1 : 1;
      nextWidth = clamp(clampedWidth + direction * step);
    }
    if (nextWidth === undefined) return;

    event.preventDefault();
    if (nextWidth === clampedWidth) return;
    event.currentTarget.parentElement?.style.setProperty(
      RESIZABLE_WIDTH_PROPERTY,
      `${nextWidth}px`,
    );
    setWidthState({ storageKey, width: nextWidth });
    try {
      setLocalStorageItem(storageKey, nextWidth, WidthSchema);
    } catch (error) {
      console.error("Could not persist panel width.", error);
    }
  };

  return { width: clampedWidth, minWidth, maxWidth, handlers: { ...handlers, onKeyDown } };
}
