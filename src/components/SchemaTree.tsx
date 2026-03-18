import { useId } from "react";
import type { SchemaNode } from "../types";

type SchemaTreeProps = {
  node: SchemaNode;
  depth: number;
  selectedDatabase: string | null;
  onInsertReference: (node: SchemaNode) => void;
  onSelectDatabase: (database: string) => void;
};

export function SchemaTree({ node, depth, selectedDatabase, onInsertReference, onSelectDatabase }: SchemaTreeProps) {
  const detailsId = useId();
  const isLeaf = !node.children || node.children.length === 0;
  const defaultOpen = depth < 2;
  const isSelectedDatabase = node.type === "database" && node.label === selectedDatabase;

  if (isLeaf) {
    return (
      <button className={`tree-node type-${node.type}`} type="button" onClick={() => onInsertReference(node)}>
        <span>{node.label}</span>
        {node.description ? <small>{node.description}</small> : null}
      </button>
    );
  }

  return (
    <details className={`tree-group type-${node.type} ${isSelectedDatabase ? "database-selected" : ""}`} open={defaultOpen}>
      <summary
        onClick={() => {
          if (node.type === "database") {
            onSelectDatabase(node.label);
          }
        }}
      >
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
            onSelectDatabase={onSelectDatabase}
          />
        ))}
      </div>
    </details>
  );
}
