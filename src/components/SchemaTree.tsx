import { useEffect, useId, useRef, useState } from "react";
import type { SchemaNode } from "../types";

type SchemaTreeProps = {
  node: SchemaNode;
  depth: number;
  selectedDatabase: string | null;
  onInsertReference: (node: SchemaNode) => void;
  onOpenTablePreview: (node: SchemaNode) => void;
  onSelectDatabase: (database: string) => void;
};

export function SchemaTree({
  node,
  depth,
  selectedDatabase,
  onInsertReference,
  onOpenTablePreview,
  onSelectDatabase,
}: SchemaTreeProps) {
  const detailsId = useId();
  const clickTimeoutRef = useRef<number | null>(null);
  const isLeaf = !node.children || node.children.length === 0;
  const defaultOpen = depth < 2;
  const isSelectedDatabase = node.type === "database" && node.label === selectedDatabase;
  const [isOpen, setIsOpen] = useState(defaultOpen);

  useEffect(() => {
    return () => {
      if (clickTimeoutRef.current !== null) {
        window.clearTimeout(clickTimeoutRef.current);
      }
    };
  }, []);

  function handleLeafClick() {
    if (node.type !== "table") {
      onInsertReference(node);
      return;
    }

    if (clickTimeoutRef.current !== null) {
      window.clearTimeout(clickTimeoutRef.current);
    }

    clickTimeoutRef.current = window.setTimeout(() => {
      onInsertReference(node);
      clickTimeoutRef.current = null;
    }, 180);
  }

  function handleLeafDoubleClick() {
    if (node.type !== "table") {
      return;
    }

    if (clickTimeoutRef.current !== null) {
      window.clearTimeout(clickTimeoutRef.current);
      clickTimeoutRef.current = null;
    }

    onOpenTablePreview(node);
  }

  function handleSummaryClick(event: React.MouseEvent<HTMLElement>) {
    if (node.type === "table") {
      event.preventDefault();

      if (clickTimeoutRef.current !== null) {
        window.clearTimeout(clickTimeoutRef.current);
      }

      clickTimeoutRef.current = window.setTimeout(() => {
        setIsOpen((current) => !current);
        clickTimeoutRef.current = null;
      }, 180);
      return;
    }

    if (node.type === "database") {
      onSelectDatabase(node.label);
    }
  }

  function handleSummaryDoubleClick(event: React.MouseEvent<HTMLElement>) {
    if (node.type !== "table") {
      return;
    }

    event.preventDefault();
    event.stopPropagation();

    if (clickTimeoutRef.current !== null) {
      window.clearTimeout(clickTimeoutRef.current);
      clickTimeoutRef.current = null;
    }

    onOpenTablePreview(node);
  }

  if (isLeaf) {
    return (
      <button
        className={`tree-node type-${node.type}`}
        type="button"
        onClick={handleLeafClick}
        onDoubleClick={handleLeafDoubleClick}
      >
        <span>{node.label}</span>
        {node.description ? <small>{node.description}</small> : null}
      </button>
    );
  }

  return (
    <details className={`tree-group type-${node.type} ${isSelectedDatabase ? "database-selected" : ""}`} open={isOpen}>
      <summary onClick={handleSummaryClick} onDoubleClick={handleSummaryDoubleClick}>
        <span>{node.label}</span>
        {node.description ? <small id={detailsId}>{node.description}</small> : null}
      </summary>
      <div className="tree-children">
        {node.children?.map((child) => (
          <SchemaTree
            key={child.id}
            node={child}
            depth={depth + 1}
            selectedDatabase={selectedDatabase}
            onInsertReference={onInsertReference}
            onOpenTablePreview={onOpenTablePreview}
            onSelectDatabase={onSelectDatabase}
          />
        ))}
      </div>
    </details>
  );
}
