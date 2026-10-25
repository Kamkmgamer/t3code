import { useLayoutEffect, useState } from "react";

import { observeResize } from "../lib/observeResize";

import { type ResizableWidthHandlers, useResizableWidth } from "./useResizableWidth";

const FILE_TREE_WIDTH_STORAGE_KEY = "t3code:diff-file-tree-width";
const FILE_TREE_DEFAULT_WIDTH = 288;
const FILE_TREE_MIN_WIDTH = 192;
/**
 * Floor for the diff column beside the tree: below it the code view is too
 * narrow to read, so the tree yields instead.
 */
const CODE_VIEW_MIN_WIDTH = 360;

export interface DiffFileTreePanelSize {
  readonly width: number;
  readonly minWidth: number;
  readonly maxWidth: number;
  readonly handlers: ResizableWidthHandlers;
}

/**
 * The row mounts only when a patch renders, so callers track it as state
 * (`const [row, setRow] = useState(null)`, `ref={setRow}`) and pass it in,
 * re-measuring when it appears rather than only on first mount.
 */
export function useDiffFileTreePanelSize(row: HTMLDivElement | null): DiffFileTreePanelSize {
  const maxWidth = useRowClampedMaxWidth(row);
  return useResizableWidth({
    storageKey: FILE_TREE_WIDTH_STORAGE_KEY,
    defaultWidth: FILE_TREE_DEFAULT_WIDTH,
    minWidth: FILE_TREE_MIN_WIDTH,
    maxWidth,
    edge: "left",
  });
}

function useRowClampedMaxWidth(row: HTMLDivElement | null): number {
  // Until the callback ref runs there is no row to measure. Leave the cap open
  // so useResizableWidth can load the persisted width; the layout effect below
  // measures and clamps it before the browser paints the row.
  const [maxWidth, setMaxWidth] = useState(Number.POSITIVE_INFINITY);
  useLayoutEffect(() => {
    if (row === null) return;
    // Measure before first paint: the persisted width must be clamped against
    // the row on the initial render, not one observer tick later (the tree
    // would flash over-wide on every mount).
    const measure = () => {
      setMaxWidth(Math.max(FILE_TREE_MIN_WIDTH, Math.floor(row.clientWidth) - CODE_VIEW_MIN_WIDTH));
    };
    measure();
    return observeResize(row, () => measure());
  }, [row]);
  return maxWidth;
}
