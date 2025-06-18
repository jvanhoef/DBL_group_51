import os

def save_plot(fig, filename, output_dir="plots"):
    os.makedirs(output_dir, exist_ok=True)
    if not filename.endswith(".png"):
        filename += ".png"
    filepath = os.path.join(output_dir, filename)
    fig.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"Saved: {filepath}")
    return filepath