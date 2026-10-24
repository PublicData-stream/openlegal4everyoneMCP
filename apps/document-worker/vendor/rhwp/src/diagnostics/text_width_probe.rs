//! [#2156] 텍스트 폭 프로브 — `estimate_text_width` 를 CLI 로 직독한다.
//!
//! 한글 유효 문자폭 통제 프로브(tools/make_width_ladder.py + probe_width_ladder.py)
//! 의 rhwp 대조축. 동일 문자열의 본 환경 측정 폭을 출력해 클래스별 편차를
//! 정량화한다.
//!
//! 사용:
//!   rhwp measure-width --size 10 [--font "함초롬바탕"] [--ratio 100] <text>...
//!   rhwp measure-width --size 10 --repeat 100 가 0 A a "(" ","
//!
//! 출력(TSV): text(축약) \t chars \t width_px \t per_char_px

use crate::renderer::layout::estimate_text_width_unrounded;
use crate::renderer::TextStyle;

pub fn run(args: &[String]) -> i32 {
    // 종료 코드 계약(mydocs/manual/cli_commands.md): 반환형이 `()` 라 인자 누락
    // (사용법 오류)에도 0 으로 끝나, 폭 사다리 스크립트가 빈 출력을 정상으로 취급했다.
    let mut font = String::from("함초롬바탕");
    let mut size_pt = 10.0f64;
    let mut ratio = 100.0f64;
    let mut repeat = 1usize;
    let mut texts: Vec<String> = Vec::new();
    let mut i = 0;
    let mut options_done = false;
    while i < args.len() {
        if options_done {
            texts.push(args[i].clone());
            i += 1;
            continue;
        }
        match args[i].as_str() {
            "--" => options_done = true,
            "--font" => {
                i += 1;
                let Some(value) = args.get(i).filter(|value| !value.starts_with('-')) else {
                    eprintln!("오류: --font 뒤에 글꼴 이름이 필요합니다.");
                    return super::EXIT_USAGE;
                };
                font = value.clone();
            }
            "--size" => {
                i += 1;
                let Some(value) = args.get(i).filter(|value| !value.starts_with('-')) else {
                    eprintln!("오류: --size 뒤에 양수 글자 크기가 필요합니다.");
                    return super::EXIT_USAGE;
                };
                let Ok(parsed) = value.parse::<f64>() else {
                    eprintln!("오류: --size 뒤에 숫자가 필요합니다 - {value}");
                    return super::EXIT_USAGE;
                };
                if parsed <= 0.0 {
                    eprintln!("오류: --size 는 0보다 커야 합니다 - {value}");
                    return super::EXIT_USAGE;
                }
                size_pt = parsed;
            }
            "--ratio" => {
                i += 1;
                let Some(value) = args.get(i).filter(|value| !value.starts_with('-')) else {
                    eprintln!("오류: --ratio 뒤에 양수 백분율이 필요합니다.");
                    return super::EXIT_USAGE;
                };
                let Ok(parsed) = value.parse::<f64>() else {
                    eprintln!("오류: --ratio 뒤에 숫자가 필요합니다 - {value}");
                    return super::EXIT_USAGE;
                };
                if parsed <= 0.0 {
                    eprintln!("오류: --ratio 는 0보다 커야 합니다 - {value}");
                    return super::EXIT_USAGE;
                }
                ratio = parsed;
            }
            "--repeat" => {
                i += 1;
                let Some(value) = args.get(i).filter(|value| !value.starts_with('-')) else {
                    eprintln!("오류: --repeat 뒤에 양의 정수가 필요합니다.");
                    return super::EXIT_USAGE;
                };
                let Ok(parsed) = value.parse::<usize>() else {
                    eprintln!("오류: --repeat 뒤에 양의 정수가 필요합니다 - {value}");
                    return super::EXIT_USAGE;
                };
                if parsed == 0 {
                    eprintln!("오류: --repeat 는 0보다 커야 합니다 - {value}");
                    return super::EXIT_USAGE;
                }
                repeat = parsed;
            }
            option if option.starts_with('-') => {
                eprintln!("오류: 알 수 없는 measure-width 옵션입니다 - {option}");
                return super::EXIT_USAGE;
            }
            t => texts.push(t.to_string()),
        }
        i += 1;
    }
    if texts.is_empty() {
        eprintln!("사용: rhwp measure-width --size 10 [--font 이름] [--repeat N] <text>...");
        return super::EXIT_USAGE;
    }
    let style = TextStyle {
        font_family: font.clone(),
        font_size: size_pt * 96.0 / 72.0,
        ratio: ratio / 100.0,
        ..TextStyle::default()
    };
    println!(
        "font={font} size={size_pt}pt ({:.3}px) ratio={ratio}%",
        style.font_size
    );
    println!("text\tchars\twidth_px\tper_char_px");
    for t in texts {
        let s = t.repeat(repeat);
        let w = estimate_text_width_unrounded(&s, &style);
        let n = s.chars().count().max(1);
        let label: String = t.chars().take(8).collect();
        println!("{label}\t{n}\t{w:.3}\t{:.4}", w / n as f64);
    }
    super::EXIT_OK
}
