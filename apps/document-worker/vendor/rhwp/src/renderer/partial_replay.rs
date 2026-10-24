//! Conservative geometry used to skip text work during partial Canvas replay.

use super::render_tree::BoundingBox;
use super::TextStyle;

/// Partial replay may use layout bounds only after adding an ink envelope.
///
/// Two line extents cover fixed line-height text whose glyph em is taller than
/// the layout box, ordinary font overhang, and decorations whose size is
/// derived from the font size. Effects with an independent ink extent opt out
/// of culling entirely at the call site.
const PARTIAL_TEXT_REPLAY_PAD_LINES: f64 = 2.0;

pub(crate) fn expanded_plain_text_replay_bounds(bbox: BoundingBox, font_size: f64) -> BoundingBox {
    let font_size = if font_size > 0.0 { font_size } else { 12.0 };
    let pad = bbox.height.max(font_size).max(1.0) * PARTIAL_TEXT_REPLAY_PAD_LINES;
    BoundingBox::new(
        bbox.x - pad,
        bbox.y - pad,
        bbox.width + pad * 2.0,
        bbox.height + pad * 2.0,
    )
}

pub(crate) fn expanded_text_replay_bounds(
    bbox: BoundingBox,
    style: &TextStyle,
    rotation: f64,
    is_vertical: bool,
    has_char_overlap: bool,
) -> Option<BoundingBox> {
    let has_independent_ink_extent = rotation.abs() > f64::EPSILON
        || is_vertical
        || has_char_overlap
        || style.italic
        || style.outline_type > 0
        || style.shadow_type > 0
        || style.emboss
        || style.engrave;
    if has_independent_ink_extent {
        return None;
    }

    Some(expanded_plain_text_replay_bounds(bbox, style.font_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_text_replay_uses_font_extent_when_line_box_is_smaller() {
        let style = TextStyle {
            font_size: 24.0,
            ..TextStyle::default()
        };
        let bounds = expanded_text_replay_bounds(
            BoundingBox::new(100.0, 200.0, 80.0, 8.0),
            &style,
            0.0,
            false,
            false,
        )
        .expect("plain text should have a finite conservative replay envelope");

        assert_eq!(bounds.x, 52.0);
        assert_eq!(bounds.y, 152.0);
        assert_eq!(bounds.width, 176.0);
        assert_eq!(bounds.height, 104.0);
    }

    #[test]
    fn partial_text_replay_never_culls_independent_ink_effects() {
        let bbox = BoundingBox::new(100.0, 200.0, 80.0, 16.0);
        for style in [
            TextStyle {
                italic: true,
                ..TextStyle::default()
            },
            TextStyle {
                outline_type: 1,
                ..TextStyle::default()
            },
            TextStyle {
                shadow_type: 1,
                shadow_offset_x: 80.0,
                shadow_offset_y: 40.0,
                ..TextStyle::default()
            },
            TextStyle {
                emboss: true,
                ..TextStyle::default()
            },
            TextStyle {
                engrave: true,
                ..TextStyle::default()
            },
        ] {
            assert!(expanded_text_replay_bounds(bbox, &style, 0.0, false, false).is_none());
        }
        assert!(
            expanded_text_replay_bounds(bbox, &TextStyle::default(), 30.0, false, false).is_none()
        );
        assert!(
            expanded_text_replay_bounds(bbox, &TextStyle::default(), 0.0, true, false).is_none()
        );
        assert!(
            expanded_text_replay_bounds(bbox, &TextStyle::default(), 0.0, false, true).is_none()
        );
    }
}
