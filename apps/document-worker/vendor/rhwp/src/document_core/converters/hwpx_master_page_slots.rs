//! HWPX 바탕쪽 적용 범위를 HWP5 저장 슬롯으로 정규화한다.
//!
//! HWPX는 `Both`/`Odd`/`Even` 바탕쪽을 희소하게 선언하고 없는 쪽은 앞 구역의
//! 바탕쪽을 상속할 수 있다. HWP5 `LIST_HEADER`도 이 희소 선언을 표현할 수 있지만,
//! 여러 적용 범위가 함께 있을 때는 저장 순서가 적용 범위를 결정한다.

use crate::model::document::Document;
use crate::model::header_footer::{HeaderFooterApply, MasterPage};

#[derive(Clone)]
struct TrackedMasterPage {
    key: (usize, usize),
    master_page: MasterPage,
}

/// HWPX의 다중 바탕쪽 선언을 HWP5 저장 순서가 표현할 수 있는 슬롯으로 정규화한다.
/// 반환값은 실제로 바뀐 구역 수다.
///
/// 한컴 2020은 단일 `Odd` 선언을 `Odd` 하나와 SECTION_DEF `0x8000_0000` 플래그로
/// 저장한다. 이 표현은 이전 구역의 짝수 바탕쪽을 계속 상속한다. 따라서 단일 선언을
/// 인위적으로 `Both + Odd`로 확장하면 안 된다. 서로 다른 적용 범위가 함께 있을 때만
/// `Both`를 먼저 두고 `Odd`가 홀수 쪽을 덮는 HWP5 순서로 물질화한다. 이 경로는 HWPX
/// 출처 어댑터에서만 호출한다.
pub(crate) fn materialize_hwp5_master_page_slots(document: &mut Document) -> u32 {
    let mut changed_sections = 0;
    let mut carry_odd: Option<TrackedMasterPage> = None;
    let mut carry_even: Option<TrackedMasterPage> = None;

    for (section_index, section) in document.sections.iter_mut().enumerate() {
        let master_pages = &mut section.section_def.master_pages;
        let base_pages: Vec<TrackedMasterPage> = master_pages
            .iter()
            .enumerate()
            .filter(|(_, master_page)| !master_page.is_extension)
            .map(|(master_page_index, master_page)| TrackedMasterPage {
                key: (section_index, master_page_index),
                master_page: master_page.clone(),
            })
            .collect();

        if base_pages.is_empty() {
            continue;
        }

        // HWP 2020의 HWPX -> HWP 저장본은 단일 Both/Odd/Even 선언을 그대로
        // LIST_HEADER 하나로 둔다. 특히 단일 Odd를 Both+Odd로 확장하면 한컴의
        // SECTION_DEF 0x80000000 상속 계약을 잃어 짝수 쪽 바탕쪽과 개체 출력이 달라진다.
        if base_pages.len() == 1 {
            update_carry(&base_pages, &mut carry_odd, &mut carry_even);
            continue;
        }

        let odd = effective_master_page(&base_pages, HeaderFooterApply::Odd, &carry_odd);
        let even = effective_master_page(&base_pages, HeaderFooterApply::Even, &carry_even);

        // 현재 조판 선택기가 어떤 쪽에도 바탕쪽을 고르지 못하는 비정상 조합은
        // 기존 저장 동작을 유지한다. 일반 HWPX의 Both/Odd/Even 조합은 여기 오지 않는다.
        let (Some(odd), Some(even)) = (odd, even) else {
            update_carry(&base_pages, &mut carry_odd, &mut carry_even);
            continue;
        };

        let mut normalized = Vec::with_capacity(2 + master_pages.len() - base_pages.len());
        let mut both = even.master_page.clone();
        both.apply_to = HeaderFooterApply::Both;
        normalized.push(both);

        if odd.key != even.key {
            let mut odd_master_page = odd.master_page.clone();
            odd_master_page.apply_to = HeaderFooterApply::Odd;
            normalized.push(odd_master_page);
        }

        normalized.extend(
            master_pages
                .iter()
                .filter(|master_page| master_page.is_extension)
                .cloned(),
        );

        let before: Vec<((usize, usize), HeaderFooterApply)> = base_pages
            .iter()
            .map(|master_page| (master_page.key, master_page.master_page.apply_to))
            .collect();
        let after: Vec<((usize, usize), HeaderFooterApply)> = normalized
            .iter()
            .filter(|master_page| !master_page.is_extension)
            .enumerate()
            .map(|(index, master_page)| {
                let key = if index == 0 { even.key } else { odd.key };
                (key, master_page.apply_to)
            })
            .collect();
        if before != after {
            changed_sections += 1;
        }

        *master_pages = normalized;
        let normalized_base: Vec<TrackedMasterPage> = master_pages
            .iter()
            .enumerate()
            .filter(|(_, master_page)| !master_page.is_extension)
            .map(|(master_page_index, master_page)| TrackedMasterPage {
                key: (section_index, master_page_index),
                master_page: master_page.clone(),
            })
            .collect();
        update_carry(&normalized_base, &mut carry_odd, &mut carry_even);
    }

    changed_sections
}

fn effective_master_page(
    base_pages: &[TrackedMasterPage],
    apply_to: HeaderFooterApply,
    carry: &Option<TrackedMasterPage>,
) -> Option<TrackedMasterPage> {
    base_pages
        .iter()
        .find(|master_page| master_page.master_page.apply_to == apply_to)
        .cloned()
        .or_else(|| {
            base_pages
                .iter()
                .find(|master_page| master_page.master_page.apply_to == HeaderFooterApply::Both)
                .cloned()
        })
        .or_else(|| carry.clone())
        .or_else(|| (base_pages.len() == 1).then(|| base_pages[0].clone()))
}

fn update_carry(
    base_pages: &[TrackedMasterPage],
    carry_odd: &mut Option<TrackedMasterPage>,
    carry_even: &mut Option<TrackedMasterPage>,
) {
    for master_page in base_pages {
        match master_page.master_page.apply_to {
            HeaderFooterApply::Both => {
                *carry_odd = Some(master_page.clone());
                *carry_even = Some(master_page.clone());
            }
            HeaderFooterApply::Odd => *carry_odd = Some(master_page.clone()),
            HeaderFooterApply::Even => *carry_even = Some(master_page.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::document::Section;
    use crate::model::paragraph::Paragraph;

    fn master_page(apply_to: HeaderFooterApply, text: &str) -> MasterPage {
        MasterPage {
            apply_to,
            paragraphs: vec![Paragraph {
                text: text.to_string(),
                ..Default::default()
            }],
            text_width: 40000,
            text_height: 50000,
            ..Default::default()
        }
    }

    #[test]
    fn sparse_odd_master_keeps_hancom_single_slot_contract() {
        let mut document = Document {
            sections: vec![
                Section {
                    section_def: crate::model::document::SectionDef {
                        master_pages: vec![
                            master_page(HeaderFooterApply::Even, "책 제목"),
                            master_page(HeaderFooterApply::Odd, "제1장"),
                        ],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                Section {
                    section_def: crate::model::document::SectionDef {
                        master_pages: vec![master_page(HeaderFooterApply::Odd, "제2장")],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(materialize_hwp5_master_page_slots(&mut document), 1);
        let saved = &document.sections[1].section_def.master_pages;
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].apply_to, HeaderFooterApply::Odd);
        assert_eq!(saved[0].paragraphs[0].text, "제2장");

        assert_eq!(materialize_hwp5_master_page_slots(&mut document), 0);
    }
}
